# Set theme for AB 32 paper ----
theme_update(axis.title.y=element_text(size=16,face="bold",angle=90),
        axis.title.x=element_text(size=16,face="bold"),
        plot.title=element_text(size=18,face="bold"),
        legend.title=element_text(size=16,face="bold"),
        legend.text=element_text(size=16),
        axis.text=element_text(size=16),
        strip.text=element_text(size=16)
        )
# Read in MySQL data tables ----
library(data.table)
library(dplyr)
#experimentsDB <- dbConnect("PostgreSQL",user="wms5f", password="manx(0)Rose",dbname="eu_experiments", host="localhost")
for (con in dbListConnections(PostgreSQL())) dbDisconnect(con)
sessions = data.table(tbl(src_postgres("eu_experiments"),sql("select * from v_sessions"))%.%collect())
auctions = tbl(src_postgres("eu_experiments"),sql("select * from v_auctions"))
auction_items = data.table(tbl(src_postgres("eu_experiments"),sql("select * from v_auction_items"))%.%collect())
bids <- tbl(src_postgres("eu_experiments"),sql("select * from v_bids"))
units <- tbl(src_postgres("eu_experiments"),sql("select * from v_units"))
subjects <- tbl(src_postgres("eu_experiments"),"subjects")

# Add key variables to the session table ----
library(car)
sessions$session_efficiency = (sessions$session_net_surplus/sessions$max_net_surplus)*100
# Auction variables ----
temp = units %.% select(auction_id,session_top_unit) %.% 
                 group_by(auction_id) %.% 
                 summarize(session_top_units = sum(session_top_unit)) %.%
                 mutate(id = auction_id) %.% select(-auction_id)%.%
                 arrange(id)
auctions = data.table(merge(auctions,temp,by="id"))
temp = auction_items[,list(s_permits_bought = sum(s_permits_bought),
                    s_permits_sold = sum(s_permits_sold),
                    s_expenditure = sum(s_expenditure),
                    s_revenue = sum(s_revenue),
                    buy_quantity = sum(buy_quantity),
                    sell_quantity = sum(sell_quantity),
                    buy_price = mean(buy_price),
                    sell_price = mean(sell_price)), by=auction_id]
setnames(temp, "auction_id", "id")
setkey(temp,id); setkey(auctions,id)
auctions <- auctions[temp]
auctions[,output_price_level := ifelse(output_price==sessions$output_price,'low','high')]
auctions$output_price_level.f = recode(auctions$output_price_level, "'low'='Low'; else='High'")
temp = sessions[,list(id,dynamic_uniform_price_prediction,session_efficiency)]
setkey(auctions,session_id); setkey(temp,id)
auctions = auctions[temp]
auctions[,`:=`( 
              current_total_bank = a_end_permits,
              auction_efficiency = (net_surplus/auction_max_session_surplus)*100,
              auction_efficiency_diff = auction_max_session_surplus-net_surplus,
              dif_price_predicted = (uniform_price - uniform_price_prediction),
              dif_price_dynamic = (uniform_price - dynamic_uniform_price_prediction),
              dif_wprice_dynamic = (uniform_price_prediction - dynamic_uniform_price_prediction),
              sdif_price_predicted = abs(uniform_price - uniform_price_prediction),
              sdif_price_dynamic = abs(uniform_price - dynamic_uniform_price_prediction),
              sdif_wprice_dynamic = abs(uniform_price_prediction - dynamic_uniform_price_prediction),
              spot_price_diff = spot_price - uniform_price,
              spot_dynamic_price_diff = spot_price - dynamic_uniform_price_prediction
              )]
auctions[,max_surplus_allocation_high := NULL]

# More session variables ----
library(gdata)
setkey(sessions,id)
vars = c("s_permits_sold","s_expenditure","buy_quantity","sell_quantity","net_surplus","auction_max_session_surplus")
temp = auctions[, lapply(.SD, sum, na.rm=TRUE), by=session_id, .SDcols=vars ][, `:=`(id=session_id,session_id=NULL)]
setkey(temp,id)
sessions <- sessions[temp]
#vars = c("spot_price","buy_price","sell_price")
#temp = auctions[, lapply(.SD, mean, na.rm=TRUE), by=session_id, .SDcols=vars ][, `:=`(id=session_id,session_id=NULL)]
#temp = aggregate(auctions[c("spot_price","buy_price","sell_price","current_trading","sdif_price_dynamic")],by=list(id=auctions$session_id),mean, na.rm=TRUE)
#sessions <- merge(sessions,temp,by="id")
# Rename spot market variables to something a little more descriptive.
spotVars_pre <- c("s_permits_sold","s_expenditure","buy_quantity","sell_quantity")
spotVars_post <- c("spot_market_volume","spot_market_expenditure","spot_market_bid_volume","spot_market_offer_volume")
auctions <- rename.vars(auctions,spotVars_pre,spotVars_post)
sessions <- rename.vars(sessions,spotVars_pre,spotVars_post)

# The auction_items data frame ----
#auction_items = auction_items%.%collect()
subjects = data.table(subjects%.%collect())
# One observation for each auction/subject pair.
# This allows us to build a panel of subjects who each participate in one session-sequence of 12 auctions
units = data.frame(units%.%collect())
units = data.table(units,key="unit_id")
units[,`:=`(
            actual_value = unit_value * used_unit,
            auction_value = unit_value * auction_top_unit,
            session_value = unit_value * session_top_unit)]
units[,`:=`(
            auction_value_diff = auction_value - actual_value,
            session_value_diff = session_value - actual_value)] 
# Aggregate from the units table to the auction_items table
vars= c("session_top_unit","auction_top_unit","actual_value","auction_value","session_value")
temp = units[, lapply(.SD, sum, na.rm=TRUE), by=auction_item_id, .SDcols=vars ][, `:=`(id=auction_item_id,auction_item_id=NULL)]
#temp = aggregate(units[vars],by=list(id=units$auction_item_id),sum, na.rm=TRUE)
auction_items <- merge(auction_items,temp,by="id",all.x=TRUE)
auction_items <- rename.vars(auction_items,c("subject_id","session_top_unit","auction_top_unit"),
                    c("subject_num","session_top_units","auction_top_units"))
subjects <- rename.vars(subjects,"id","subject_id")
temp = subjects[,list(subject_num,session_id,subject_id)]
setkey(auction_items,"session_id","subject_num")
setkey(temp,"session_id","subject_num")
auction_items = auction_items[temp]
temp = sessions[,list(id,pcr_method,dynamic_uniform_price_prediction)]
setkey(auction_items,session_id)
auction_items = auction_items[temp]
auction_items[,`:=`(
                  session_production_diff = production - session_top_units,
                  auction_production_diff = production - auction_top_units,
                  net_actual_value = actual_value - permits_used * dynamic_uniform_price_prediction,
                  net_auction_value = auction_value - permits_used * dynamic_uniform_price_prediction,
                  net_session_value = session_value - permits_used * dynamic_uniform_price_prediction
                  )]
auction_items$lost_value = auction_items$net_actual_value - auction_items$net_session_value
auction_items$value_dif <- auction_items$net_session_value - auction_items$net_actual_value
vars = c("actual_value","auction_value","session_value","net_actual_value","net_auction_value",
        "net_session_value","auction_production_diff","session_production_diff","permits_used")
temp = auction_items[, lapply(.SD, sum, na.rm=TRUE), by=auction_num, .SDcols=vars ][, `:=`(id=auction_num,auction_num=NULL)]
#temp = aggregate(auction_items[,list(var)],by=list(id=auction_items$auction_id),sum, na.rm=TRUE)
auctions = merge(auctions,temp,by="id")

# Aggregate bid information ----
bids = data.table(bids%.%collect())
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
  num_bids_accepted = sum(num_bids_accepted,na.rm=TRUE)),
  by = session_id][, `:=`(id=session_id,session_id=NULL)]
sessions <- merge(sessions,temp,by="id",all.x=TRUE)
 
#xtabs( ~ subjectNum + auctionNum, data=auction_items)
#xtabs( ~ session_id + auction, data=auctions)
# End data generation
