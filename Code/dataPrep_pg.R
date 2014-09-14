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
experimentsDB <- dbConnect(MySQL(),user="root", password="",dbname="Experiments", host="localhost")
bids <- mysqlReadTable(experimentsDB, "v_bids", row.names = FALSE)
units <- mysqlReadTable(experimentsDB, "v_units", row.names = FALSE)
auctionItems <- mysqlReadTable(experimentsDB, "v_auctionitems", row.names = FALSE)
auctions <- mysqlReadTable(experimentsDB, "v_auctions", row.names = FALSE)
subjects <- mysqlReadTable(experimentsDB, "subjects", row.names = FALSE)
subjects <- rename.vars(subjects,"id","subjectNum")
sessions <- mysqlReadTable(experimentsDB, "v_sessions", row.names = FALSE)
dbDisconnect(experimentsDB)

# Add key variables to the session table ----
sessions$holding_limit.f = factor(sessions$holding_limit, levels = c("tight","loose"), labels=c("Tight","Loose"))
sessions$reserve_method.f = factor(sessions$compliance_reserve_method, levels = c("post auction","auction"), labels=c("Post auction","In auction"))
sessions$price_method.f = factor(sessions$uniform_price_method, levels = c("highest rejected bid","lowest accepted bid"), labels=c("Highest rejected bid","Lowest accepted bid"))
sessions$pcr_method.f = factor(sessions$pcr_method, levels = c(0,1), labels=c("Reallocated tiers","Fixed tiers"))
sessions$sessionEfficiency = (sessions$session_net_surplus/sessions$max_net_surplus)*100
holding_limit_levels = c("Tight","Loose"); holding_limit_names=c("Tight","Loose")

# Auction variables ----
temp = aggregate(units["session_top_unit"],by=list(id=units$auction_id),sum)
auctions = merge(auctions,temp,by="id")
var = c("s_permits_bought","s_permits_sold","s_expenditure","s_revenue","buy_quantity","sell_quantity")
temp = aggregate(auctionItems[var],by=list(id=auctionItems$auction_id),sum, na.rm=TRUE)
auctions = merge(auctions,temp,by="id")
temp = aggregate(auctionItems[c("buy_price","sell_price")],by=list(id=auctionItems$auction_id),mean, na.rm=TRUE)
auctions <- merge(auctions,temp,by="id")
auctions <- rename.vars(auctions,"session_top_unit","session_top_units")
auctions$low_hydro_year = auctions$output_price
auctions$low_hydro_year.f = recode(auctions$low_hydro_year, "c(35)='Normal'; else='Low hydro'")
auctions = merge(auctions,sessions[c("id","holding_limit.f","reserve_method.f","price_method.f","pcr_method.f","dynamic_uniform_price_prediction","session_group","sessionEfficiency")],by.x="session_id",by.y="id",all.x=TRUE)
auctions = transform(auctions, 
              current_trading = a_end_permits - a_compliance_carry_over,
              current_total_bank = a_end_permits,
              auctionEfficiency = (netSurplus/auctionMaxSessionSurplus)*100,
              auctionEfficiency_diff = auctionMaxSessionSurplus-netSurplus,
              trading_pct = ((a_end_permits - a_compliance_carry_over)/a_end_permits)*100,
              dif_price_predicted = (uniform_price - uniform_price_prediction),
              dif_price_dynamic = (uniform_price - dynamic_uniform_price_prediction),
              dif_wprice_dynamic = (uniform_price_prediction - dynamic_uniform_price_prediction),
              sdif_price_predicted = abs(uniform_price - uniform_price_prediction),
              sdif_price_dynamic = abs(uniform_price - dynamic_uniform_price_prediction),
              sdif_wprice_dynamic = abs(uniform_price_prediction - dynamic_uniform_price_prediction),
              spot_price_diff = spot_price - uniform_price,
              spot_dynamic_price_diff = spot_price - dynamic_uniform_price_prediction
              )
auctions$max_surplus_allocation_high <- NULL

# More session variables ----
temp = aggregate(auctions[c("s_permits_sold","s_expenditure","buy_quantity","sell_quantity","netSurplus","auctionMaxSessionSurplus")],by=list(id=auctions$session_id),sum, na.rm=TRUE)
sessions <- merge(sessions,temp,by="id")
temp = aggregate(auctions[c("spot_price","buy_price","sell_price","current_trading","sdif_price_dynamic")],by=list(id=auctions$session_id),mean, na.rm=TRUE)
sessions <- merge(sessions,temp,by="id")
# Rename spot market variables to something a little more descriptive.
spotVars_pre <- c("s_permits_sold","s_expenditure","buy_quantity","sell_quantity","buy_price","sell_price")
spotVars_post <- c("spot_market_volume","spot_market_expenditure","spot_market_bid_volume","spot_market_offer_volume","spot_avg_bid_price","spot_avg_offer_price")
auctions <- rename.vars(auctions,spotVars_pre,spotVars_post)
sessions <- rename.vars(sessions,spotVars_pre,spotVars_post)

# The auctionItems data frame ----
# One observation for each auction/subject pair.
# This allows us to build a panel of subjects who each participate in one session-sequence of 12 auctions
units <- transform(units,
                   actual_value = unit_value * used_unit,
                   auction_value = unit_value * auction_top_unit,
                   session_value = unit_value * session_top_unit)
units <- transform(units,
                   auction_value_diff = auction_value - actual_value,
                   session_value_diff = session_value - actual_value ) 
# Aggregate from the units table to the auctionItems table
vars= c("session_top_unit","auction_top_unit","actual_value","auction_value","session_value")
temp = aggregate(units[vars],by=list(id=units$auction_item_id),sum, na.rm=TRUE)
auctionItems <- merge(auctionItems,temp,by="id")
auctionItems <- rename.vars(auctionItems,c("session_top_unit","auction_top_unit"),c("session_top_units","auction_top_units"))
auctionItems <- merge(auctionItems,subjects[c("subjectNum","subject_id","session_id")],by=c("session_id","subject_id"),all.x=TRUE)
auctionItems <- merge(auctionItems,sessions[c("id","holding_limit.f","reserve_method.f","price_method.f","pcr_method.f"
                                              ,"dynamic_uniform_price_prediction")],by.x="session_id",by.y="id",all.x=TRUE)
auctionItems <- transform(auctionItems,
                          session_production_diff = production - session_top_units,
                          auction_production_diff = production - auction_top_units,
                          net_actual_value = actual_value - permits_used * dynamic_uniform_price_prediction,
                          net_auction_value = auction_value - permits_used * dynamic_uniform_price_prediction,
                          net_session_value = session_value - permits_used * dynamic_uniform_price_prediction
                          )
auctionItems$lost_value = auctionItems$net_actual_value - auctionItems$net_session_value
auctionItems$low_hydro_year.f = recode(auctionItems$output_price,as.factor=TRUE, "c(35)='Normal'; else='Low hydro'")
auctionItems$current_trading <- auctionItems$end_permits - auctionItems$compliance_carry_over
auctionItems$value_dif <- auctionItems$net_session_value - auctionItems$net_actual_value
var = c("actual_value","auction_value","session_value","net_actual_value","net_auction_value",
        "net_session_value","auction_production_diff","session_production_diff","permits_used")
temp = aggregate(auctionItems[var],by=list(id=auctionItems$auction_id),sum, na.rm=TRUE)
auctions = merge(auctions,temp,by="id")

# Aggregate bid information ----
temp = aggregate(bids["permit_number"],by=list(id=bids$auction_item_id),max, na.rm=TRUE)
auctionItems <- merge(auctionItems,temp,by="id",all.x=TRUE)
temp = aggregate(bids["bid"],by=list(id=bids$auction_item_id),mean, na.rm=TRUE)
auctionItems <- merge(auctionItems,temp,by="id",all.x=TRUE)
temp = aggregate(bids["bid_accepted"],by=list(id=bids$auction_item_id),sum, na.rm=TRUE)
auctionItems <- merge(auctionItems,temp,by="id",all.x=TRUE)
auctionItems <- rename.vars(auctionItems,c("permit_number","bid","bid_accepted"),c("num_bids","average_bid","num_bids_accepted"))
# Aggregate bid information up to the auctions and sessions level
auctionItems$num_bids[is.na(auctionItems$num_bids)] <- 0
temp = aggregate(auctionItems[c("num_bids","num_bids_accepted")],by=list(id=auctionItems$auction_id),sum, na.rm=TRUE)
auctions <- merge(auctions,temp,by="id",all.x=TRUE)
temp = aggregate(auctionItems["average_bid"],by=list(id=auctionItems$auction_id),mean, na.rm=TRUE)
auctions <- merge(auctions,temp,by="id",all.x=TRUE)
temp = aggregate(auctions[c("num_bids","num_bids_accepted")],by=list(id=auctions$session_id),sum, na.rm=TRUE)
sessions <- merge(sessions,temp,by="id",all.x=TRUE)
temp = aggregate(auctions["average_bid"],by=list(id=auctions$session_id),mean, na.rm=TRUE)
sessions <- merge(sessions,temp,by="id",all.x=TRUE)
sessions <- within(sessions,{
  group <- NA
  group[pcr_method.f=="Reallocated tiers" & reserve_method.f=="Post auction"] = 1
  group[pcr_method.f=="Fixed tiers" & reserve_method.f=="Post auction" & price_method.f=="Highest rejected bid"] = 2
  group[pcr_method.f=="Fixed tiers" & reserve_method.f=="Post auction" & price_method.f=="Lowest accepted bid"] = 3
#  group[pcr_method.f=="Fixed tiers" & reserve_method.f=="In auction" ] = 4  
})
sessions$group.f = factor(sessions$group,levels=c(1,2,3),labels=c("Loose cap","Second price","First price"))
auctions = merge(auctions,sessions[c("id","group.f")],by.x="session_id",by.y="id",all.x=TRUE)
 
#xtabs( ~ subjectNum + auctionNum, data=auctionItems)
#xtabs( ~ session_id + auction, data=auctions)
# End data generation
