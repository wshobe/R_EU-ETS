# Set theme for paper ----
library(ggplot2)
theme_set(theme_bw())
theme_update(axis.title.y=element_text(size=16,face="bold",angle=90),
        axis.title.x=element_text(size=16,face="bold"),
        plot.title=element_text(size=18,face="bold"),
        legend.title=element_text(size=16,face="bold"),
        legend.text=element_text(size=16),
        axis.text=element_text(size=14),
        strip.text=element_text(size=16)
        )
# Read in data tables from database ----
library(RPostgreSQL)
library(dplyr)
library(data.table)
library(gdata)

#experimentsDB <- dbConnect("PostgreSQL",user="wms5f", password="manx(0)Rose",dbname="eu_experiments", host="localhost")
for (con in dbListConnections(PostgreSQL())) dbDisconnect(con)
sessions = data.table(tbl(src_postgres("eu_experiments"),sql("select * from v_sessions "))%>%collect(),key="id")
auctions = data.table(tbl(src_postgres("eu_experiments"),sql("select * from v_auctions "))%>%collect(),key="session_id")
auction_items = data.table(tbl(src_postgres("eu_experiments"),sql("select * from v_auction_items"))%>%collect(),key="session_id")
bids <- data.table(tbl(src_postgres("eu_experiments"),sql("select * from v_bids"))%>%collect(),key="session_id")
units <- data.table(tbl(src_postgres("eu_experiments"),sql("select * from v_units"))%>%collect(),key="session_id")
subjects <- data.table(tbl(src_postgres("eu_experiments"),"subjects")%>%collect())
items <- data.table(tbl(src_postgres("eu_experiments"),"items")%>%collect())

#all_items <- data.table(tbl(src_postgres("eu_experiments"),"items")%.%collect())
# Add key variables to the session table ----
#library(car)
treatmentCodebook = fread('./Data/designCodebook.csv')
setkey(sessions,session_name);setkey(treatmentCodebook,session_name)
sessions[, c("min_value","max_value","compliance_reserve_method","pcr_method","uniform_price_method","seed_value",
             "low_emitter_range_low","low_emitter_range_high","high_emitter_range_low","high_emitter_range_high",
             "auction_quantity_reduction","permit_price_cap_increment","output_price_increment","output_price_increment_probability",
             "must_serve_units_low","must_serve_units_high","must_serve_increment_low","must_serve_increment_high",
             "auction_quantity_increase","number_units","max_price","permit_endowment_low", "permit_endowment_high"):=NULL]
sessions = sessions[treatmentCodebook[usable_session=="t"]]
#sessions = sessions[session_group!="C"]
sessionIDs = sessions[,list(id)];
auctions = auctions[sessionIDs]
auction_items=auction_items[sessionIDs]
bids = bids[sessionIDs]
units = units[sessionIDs]

sessions$session_efficiency = (sessions$session_net_surplus/sessions$max_net_surplus)*100

# Auction variables ----
auctions[,c("max_surplus_allocation_high","auction_max_session_surplus"):= NULL]
dynamic_price = sessions$dynamic_uniform_price_prediction
temp = units[,list(session_top_units = sum(session_top_unit, na.rm = TRUE)
                   ),by=auction_id][, `:=`(id=auction_id,auction_id=NULL)]
auctions = merge(auctions,temp,by="id")
temp = auction_items[,list(s_permits_bought = sum(s_permits_bought),
                    s_permits_sold = sum(s_permits_sold),
                    s_expenditure = sum(s_expenditure),
                    s_revenue = sum(s_revenue),
                    buy_quantity = sum(buy_quantity),
                    sell_quantity = sum(sell_quantity),
                    mean_buy_price = mean(buy_price),
                    mean_sell_price = mean(sell_price)), by=auction_id]
setnames(temp, "auction_id", "id")
setkey(temp,id); setkey(auctions,id)
auctions <- auctions[temp]
auctions[,output_price_level := ifelse(output_price==30,'low','high')]
#auctions$output_price_level.f = recode(auctions$output_price_level, "'low'='Low'; else='High'")
temp = sessions[,list(id,dynamic_uniform_price_prediction,session_efficiency)]
setkey(auctions,session_id); setkey(temp,id)
auctions = auctions[temp]
auctions[,`:=`( 
              current_total_bank = a_end_permits,
              dif_price_predicted = (uniform_price - uniform_price_prediction),
              dif_price_dynamic = (uniform_price - dynamic_uniform_price_prediction),
              dif_wprice_dynamic = (uniform_price_prediction - dynamic_uniform_price_prediction),
              sdif_price_predicted = abs(uniform_price - uniform_price_prediction),
              sdif_price_dynamic = abs(uniform_price - dynamic_uniform_price_prediction),
              sdif_wprice_dynamic = abs(uniform_price_prediction - dynamic_uniform_price_prediction),
              spot_price_diff = spot_price - uniform_price,
              spot_dynamic_price_diff = spot_price - dynamic_uniform_price_prediction
              )]
#auctions[,max_surplus_allocation_high := NULL]

# More session variables ----
setkey(sessions,id)
vars = c("s_permits_sold","s_expenditure","buy_quantity","sell_quantity","net_surplus","cap")
temp = auctions[, lapply(.SD, sum, na.rm=TRUE), by=session_id, .SDcols=vars ][, `:=`(id=session_id,session_id=NULL,session_total_cap=cap,cap=NULL)]
sessions[,session_total_cap := NULL]
setkey(temp,id)
sessions <- sessions[temp]
# Rename spot market variables to something a little more descriptive.
spotVars_pre <- c("s_permits_sold","s_expenditure","buy_quantity","sell_quantity")
spotVars_post <- c("spot_market_volume","spot_market_expenditure","spot_market_bid_volume","spot_market_offer_volume")
setnames(auctions,spotVars_pre,spotVars_post)
setnames(sessions,spotVars_pre,spotVars_post)

# The auction_items data frame ----
# One observation for each auction/subject pair.
# This allows us to build a panel of subjects who each participate in one session-sequence of 12 auctions

temp = sessions[,list(last_auction,session_total_cap,dynamic_uniform_price_prediction,
                      compliance_reserve_quantity,session_id =id)]
setkey(units,session_id);setkey(temp,session_id)
units = units[temp]
units[,c("session_top_unit","auction_top_unit","unit_emission_value") := 
        list(ifelse(is.na(session_top_unit),0,session_top_unit),
             ifelse(is.na(auction_top_unit),0,auction_top_unit),
             dynamic_uniform_price_prediction*permits_per_unit
        )]
units[,`:=`(
            actual_value = unit_value * used_unit,
            auction_value = unit_value * auction_top_unit,
            session_value = unit_value * session_top_unit,
            expected_permits_used = ifelse(unit==1,permits_per_unit,session_top_unit*permits_per_unit),
            expected_production = ifelse(unit==1,1,session_top_unit),
            net_value = unit_value - unit_emission_value
            )]
units[,`:=`(
            unit_max_session_surplus = net_value*session_top_unit,
            unit_actual_surplus = net_value*used_unit,
            prod_diff = ifelse(unit==1,0,session_top_unit-used_unit),
            lost_value_hi = ifelse(session_top_unit==1 & used_unit==0,abs(net_value),0),
            lost_value_lo = ifelse(session_top_unit==0 & used_unit==1,abs(net_value),0)
            )]
units[,`:=`(
  unit_loss = ifelse(unit==1,0,abs(unit_max_session_surplus - unit_actual_surplus)),
    prod_emission_diff = prod_diff*permits_per_unit
    ) ]

# Aggregate from the units table to the auction_items table
vars= c("session_top_unit","actual_value","auction_value","session_value",
        "expected_permits_used","unit_max_session_surplus","unit_actual_surplus","prod_diff",
        "unit_loss","used_unit","expected_production","prod_emission_diff")
temp = units[, lapply(.SD, sum, na.rm=TRUE), by=auction_item_id, .SDcols=vars ] #[, `:=`(id=auction_item_id,auction_item_id=NULL)]
setnames(temp,"auction_item_id","id")
auction_items <- merge(auction_items,temp,by="id",all.x=TRUE)
setnames(auction_items,
         c("session_top_unit","unit_max_session_surplus","unit_actual_surplus","used_unit"),
         c("session_top_units","max_session_surplus","actual_surplus","production"))

setnames(subjects,"id","subject_id")

temp = sessions[,list(id,dynamic_uniform_price_prediction)]
setkey(auction_items,session_id)
auction_items = auction_items[temp]
auction_items[,`:=`(
                  session_production_diff = production - expected_production,
                  net_actual_value = actual_value - permits_used * dynamic_uniform_price_prediction,
                  net_session_value = session_value - permits_used * dynamic_uniform_price_prediction
                  )]
auction_items$lost_value = auction_items$net_actual_value - auction_items$net_session_value
vars = c("actual_value","session_value","net_actual_value","net_session_value","session_production_diff",
        "expected_permits_used","expected_production","prod_diff","prod_emission_diff",
        "max_session_surplus","actual_surplus")
temp = auction_items[, lapply(.SD, sum, na.rm=TRUE), by=auction_id, .SDcols=vars ]   #[, `:=`(id=auction_id,auction_id=NULL)]
setnames(temp,"auction_id","id")
auctions = merge(auctions,temp,by="id",all.x=TRUE)
auctions[,`:=`(ending_reserve = compliance_reserve_stock - reserve_released,
               prod_emission_ratio = prod_emission_diff/expected_permits_used,
               emission_ratio = a_permits_used/expected_permits_used,
               auction_efficiency = (net_surplus/max_session_surplus)*100,
               auction_efficiency_diff = max_session_surplus-net_surplus
)]
setkey(sessions,id);setkey(auctions,session_id)
auctions = auctions[sessions[,list(id,treatment,policy,session_group,trial)]]


# Aggregate bid information ----
#bids = data.table(bids%.%collect())
temp = bids[,list(
    num_bids = max(permit_number,na.rm=TRUE),
    average_bid = mean(bid,na.rm=TRUE),
    num_bids_accepted = sum(bid_accepted,na.rm=TRUE)), 
    by = auction_item_id][, `:=`(id=auction_item_id,auction_item_id=NULL)]
auction_items <- merge(auction_items,temp,by="id",all.x=TRUE)
auction_items$num_bids[is.na(auction_items$num_bids)] <- 0
# Aggregate bid information up to the auctions and sessions level
temp = auction_items[,list(
  num_bids = sum(num_bids,na.rm=TRUE),
  average_bid = mean(average_bid,na.rm=TRUE),
  num_bids_accepted = sum(num_bids_accepted,na.rm=TRUE)), 
  by = auction_id][, `:=`(id=auction_id,auction_id=NULL)]
auctions <- merge(auctions,temp,by="id",all.x=TRUE)
temp = auctions[,list(
  num_bids=sum(num_bids,na.rm=TRUE),
  average_bid = mean(average_bid,na.rm=TRUE),
  num_bids_accepted = sum(num_bids_accepted,na.rm=TRUE),
  max_price = max(uniform_price),
  max_bank = max(current_total_bank,na.rm=TRUE),
  max_reserve = max(compliance_reserve_stock,na.rm=TRUE)
  ),
  by = session_id]       #[, `:=`(id=session_id,session_id=NULL)]
setnames(temp,"session_id","id")
sessions <- merge(sessions,temp,by="id",all.x=TRUE)
setkey(sessions,id);setkey(auctions,session_id)
auctions[sessions[,list(id,last_auction)],last_auction:=last_auction]
sessions[auctions[auction == last_auction,list(current_total_bank,ending_reserve,session_id)],
         `:=`(ending_reserve=ending_reserve,
              ending_bank = current_total_bank
         )]
setnames(sessions,
         c("dynamic_sum_max_value","static_surplus","max_net_surplus","session_net_surplus"),
         c("max_session_gross_surplus","static_gross_surplus","efficient_surplus","actual_surplus"))
# End ----
# End data generation
