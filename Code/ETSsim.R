
require(taRifx)    # For 'shift' function
require(dplyr)
library(RPostgreSQL)
library(data.table)
library(DBI)
rm(list = ls(all = TRUE)) 
#i = 1
# set up session variables
sessionData = fread("simValues2.csv")
num_sessions = nrow(sessionData)
# During testing, truncate the output tables before proceeding
con <- dbConnect("PostgreSQL",user="wms5f", password="manx(0)Rose",dbname="eu_experiments", host="localhost")
#dbGetQuery(con, "DELETE FROM sim_item;")
#dbGetQuery(con, "DELETE FROM sim_session;")
#dbGetQuery(con, "DELETE FROM sim_period;")
#dbDisconnect(con)
# 
# Main session loop
bidKicker = 1.1
for (session_num in 13:18) {                #sessionData$id    for all rows in the spreadsheet
#session_num=16
print(session_num)
session = sessionData[id==session_num]
set.seed(1234)
session$num_subjects = session$num_low_users + session$num_high_users
attach(session)
  session$initial_bank = low_user_init_bank*num_low_users + high_user_init_bank*num_high_users
  session$full_capacity = num_low_users*low_user_capacity*low_user_intensity + num_high_users*high_user_capacity*high_user_intensity
  session$session_data_length = num_periods*num_subjects*num_values
  #
  period = data.table(period_num=1:num_periods,key="period_num")
  period$output_price = rep(base_output_price,num_periods) + rbinom(num_periods,1,high_output_price_probability)*10
  period$output_price = c(30,30,30,40,40,40,30,30,40,40,40,30,30,40,30,40,40,30)
  period$cap = seq(initial_cap_amount,by=-cap_reduction,length.out=num_periods)
  period$session_id = session_num
  period$treatment = treatment
detach(session)
period[,`:=`(
              auction_price = 0, auction_quantity_sold=0, auction_revenue = 0
              )]
period[period_num==1,`:=`(
              starting_total_bank=session$initial_bank,
              starting_reserve = session$initial_reserve
              )]
session$num_high_price_periods = period[output_price>session$base_output_price,length(output_price)]
#
session$total_cap = period[,sum(cap)]
attach(session)
  subject_ids = rep(1:num_subjects, times=num_periods)
  item = data.table(item_id=array(1:session_data_length),key="item_id")
  item$subject_id = rep(subject_ids, each=num_values)
  item$period_num = rep(period$period_num,each=num_subjects*num_values)
  item$session_id = session_num
  low_units = rep(c(rep(1,low_user_capacity*low_user_intensity),rep(0,num_values-low_user_capacity*low_user_intensity)),times=num_low_users)
  high_units = rep(c(rep(1,high_user_capacity*high_user_intensity),rep(0,num_values-high_user_capacity*high_user_intensity)),times=num_high_users)
  item$unit = rep(c(low_units,high_units),times=num_periods)  #for high users each entry is a half capacity unit
  item[,unit_intensity:=rep(c(rep(low_user_intensity,num_low_users*num_values),rep(high_user_intensity,num_high_users*num_values)),times=num_periods)]
# Assumes only 2 emission intensities
#  item[unit_intensity==low_user_intensity,rnd_cost_factor:=runif(num_values*num_low_users*num_periods)]
#  item[unit_intensity==high_user_intensity,rnd_cost_factor:=rep(runif(num_values*num_high_users*num_periods),each=2,)]
# Set up subject data table [num_subjects*num_periods,]
# Costs are for production units only, set other values in 'for t' loop
  costs = tbl(src_postgres("eu_experiments"),"items")
  test <- costs %.%
    mutate(unit_num = unit, period_num=auction_num,subject_id=subject_id) %.%
    select(period_num,subject_id,unit_num,cost) %.%
    filter(!is.na(cost), session_id == 5) 
  test1 = data.table(collect(test))
#  setkey(item,period_num,subject_id,unit_value)
# Assign unit numbers from high to low, since sorting of values is from low to high. So unit 1 has the highest value, etc.
  item[unit==1 & unit_intensity==low_user_intensity,unit_num:=rep(1:low_user_capacity,times=(num_low_users*num_periods))]
  item[unit==1 & unit_intensity==high_user_intensity,unit_num:=rep(1:high_user_capacity,each=2,times=(num_high_users*num_periods))]
item = merge(item,test1,by=c("period_num","subject_id","unit_num"),all=TRUE)
item[unit_intensity==high_user_intensity,cost:=cost/unit_intensity]
#item[,cost:=round(unit*ifelse(unit_intensity==low_user_intensity,(rnd_cost_factor*20+10),(rnd_cost_factor*30)/unit_intensity),digits=2)]
  setkey(item,period_num);setkey(period,period_num)
  item[period,unit_value:=unit*(output_price/unit_intensity-cost)]
  item[is.na(unit_value),unit_value:=0]
  item[,unit_used:=0]
  
#  setkey(item,period_num,subject_id,unit_value)
# Assign unit numbers from high to low, since sorting of values is from low to high. So unit 1 has the highest value, etc.
#  item[unit==1 & unit_intensity==low_user_intensity,unit_num:=rep(low_user_capacity:1,times=(num_low_users*num_periods))]
#  item[unit==1 & unit_intensity==high_user_intensity,unit_num:=rep(high_user_capacity:1,each=2,times=(num_high_users*num_periods))]
  subject = data.table(id = array(1:(num_subjects*num_periods)),key="id") #One record per period.
  subject[,`:=`(subject_id=subject_ids,
                period_num=rep(1:num_periods,each=num_subjects),
                starting_bank=0, starting_cash=0, earnings=0, ending_cash=0,
                ending_bank = 0,                                                 #       starting_deficit=0,ending_deficit=0, a deficit is a negative bank amouunt
                permits_purchased=0, permits_used=0
          )]
  subject[period_num==1,`:=`(starting_bank=c(rep(low_user_init_bank,times=num_low_users),rep(high_user_init_bank,times=num_high_users)),
                           starting_cash=c(rep(low_user_init_capital,times=num_low_users),rep(high_user_init_capital,times=num_high_users))
                           )]
  subject[,unit_intensity := rep(c(rep(low_user_intensity,num_low_users),rep(high_user_intensity,num_high_users)),times=num_periods)]
#
  item = item[order(-unit_value)]
  session$dynamic_efficient_price = item[total_cap+initial_bank+1,unit_value]
  session_dynamic_value = session$dynamic_efficient_price
  item$dynamic_producing_unit=0
  item[1:(total_cap+initial_bank),dynamic_producing_unit:=1]
  session$efficient_surplus = item[dynamic_producing_unit==1,sum(unit_value-session_dynamic_value)]
detach(session)
thisBehavior = session$behavior
#

for (t in period$period_num) {     #period$period_num
#t = 1
  this_period = copy(period[t])
  t.subject = copy(subject[period_num==t])
  t.cap = this_period$cap
  if (t==1) {
    current_reserve = session$initial_reserve
    t.bank = this_period$starting_total_bank
    t.subject$bank = t.subject$starting_bank
    t.subject$cash = t.subject$starting_cash
#    subject_banks = subject[period_num==1,list(starting_bank)]    #    Already done above
#    subjectsCash = subject[period_num==1,list(starting_cash)]
  } else {
    current_reserve = period$ending_reserve[t-1]
    t.bank = period$ending_total_bank[t-1]
    period[t]$starting_total_bank = t.bank
    period[t]$starting_reserve = current_reserve
    setkey(subject,period_num)
    t.subject$bank = subject[period_num==t-1,list(ending_bank)]
    t.subject$starting_bank = t.subject$bank
    t.subject$cash = subject[period_num==t-1,list(ending_cash)]
    t.subject$starting_cash = t.subject$cash
#    st.bank = t.subjects[,list(subject_id,starting_bank)]
  }
  t.subject[,low_bank_bid_kicker:=ifelse(starting_bank<=unit_intensity*session$low_user_capacity,bidKicker,1.0)]
  this_period$starting_reserve = current_reserve
  this_reserve_price = session$reserve_price       # Can be zero
  reserve_adjustment = 0    # No liquidity collar
  # The following code implements the liquidity collar
  if(session$liquidity_collar & t>2) {
    if (period$ending_total_bank[t-2]<session$trigger_quantity_lower_bound ) {
      #Bank is small, pull permits out of the reserve
      reserve_adjustment = min(current_reserve,session$trigger_quantity_injection)
    } else if (period$ending_total_bank[t-2]>session$trigger_quantity_upper_bound) {
      #bank is too big, put permits into the reserve
      reserve_adjustment = -1*floor(period$ending_total_bank[t-2]*session$trigger_quantity_reduction_proportion)
    } 
  }
#  cat(sprintf("ReserveAdjustment: %d\n", reserve_adjustment))
  current_reserve = max(0,current_reserve - reserve_adjustment)
  auction_quantity = t.cap +  reserve_adjustment

#cat(sprintf("bank_demand_shift: %d\n", bank_demand_shift))
  setkey(item,period_num)
  st.bids = copy(item[.(t),list(item_id,unit_value,unit,subject_id,unit_num)])
#  t.bids = t.bids[order(-value)]
  st.bids = copy(st.bids[order(subject_id,-unit_value)])
  st.bids[,value:=unit_value]
#thisBehavior = "perfect foresight"
switch ( thisBehavior,
    "perfectly myopic" = {
              min_bid_value = 0
              t.subject[,low_bank_bid_kicker:=1.0]
              st.bids[unit==0,value:=min_bid_value]
                          },
    "perfect foresight" = {
              t.subject[,low_bank_bid_kicker:=ifelse(starting_bank<=unit_intensity*session$low_user_capacity,bidKicker,1.0)]
              min_bid_value = session$dynamic_efficient_price
              st.bids[,value:=ifelse(unit_value<min_bid_value,min_bid_value,unit_value)]
                          },
    stop("Please set a behavior.")  
    )
  st.bids = merge(st.bids,t.subject[,list(subject_id,starting_bank,low_bank_bid_kicker)],by="subject_id")
# Allowances in the bank come off the top of allowance values for all behaviors (could change this...)
  s.bank_demand_shift = t.subject[,ifelse(bank<0,0,bank)]
  for (i in 1:session$num_subjects) {
    ifelse(s.bank_demand_shift[i] < session$num_values,
            st.bids[subject_id==i,bid_value:=shift(st.bids[subject_id==i,value],s.bank_demand_shift[i],wrap=F,pad=T)],
            st.bids[subject_id==i,bid_value:=NA])
  }
  st.bids[,bid_value:=ifelse(is.na(bid_value) ,min_bid_value*low_bank_bid_kicker,bid_value*low_bank_bid_kicker)]

  st.bids[,bid_value:=ifelse(bid_value < min_bid_value ,min_bid_value,bid_value)]  
#  bank_demand_shift = ifelse(t.bank>session$full_capacity,session$full_capacity,t.bank)   # in Myopic sessions, use bank ASAP
#  bank_demand_shift = min(t.bank,session$full_capacity)  # Not sure this is needed. 
#  t.bids[,bid_value:=shift(t.bids[,value],bank_demand_shift,wrap=F,pad=T)]
# If bid_value is NA or lower than the dyn eff price, set it equal to dyn eff price
#  t.bids[,bid_value:=ifelse(is.na(bid_value) ,min_bid_value,bid_value)]
#  t.bids[,bid_value:=ifelse(bid_value < min_bid_value ,min_bid_value,bid_value)]
#
  switch ( thisBehavior,
      "perfectly myopic" = { # 
                             st.bids[,bid:=ifelse(unit==1,bid_value,NA)]
                             },     # In this simulation, there is no banking, so bid only on unit values
      "perfect foresight" = {   
                              st.bids[,bid:=bid_value]
                              },                    # Bid values for all 'num_values' available bids
      stop("Please set a behavior.")  
  )  
#  t.bids[,bid:=ifelse(bid>=this_reserve_price,bid,NA)]  # No bids less than the reserve price are allowed. NA is no bid
  st.bids[,bid:=ifelse(bid>=this_reserve_price,bid,NA)]
#  currentPermitValue = ifelse(is.na(t.bids$value[auction_quantity + t.bank ]),0,t.bids$value[auction_quantity])
#  setkey(t.bids,period_num)
# Auction reconciliation
# This is where the upper price collar function goes
#cat(sprintf("auction_quantity: %d\n", auction_quantity))
st.bids = st.bids[order(-bid)]
#auction_price = t.bids$bid[auction_quantity + 1 ]
auction_price =st.bids[order(-bid)][auction_quantity + 1 ,bid]   # !!!!
if (session$price_cap) {                  # If price collar upper price is set; reserve price is turned off by setting it to zero.
  if(!is.na(auction_price) & auction_price > session$high_price_limit ) {
    reserve_bid_index = which(st.bids[,bid]<=session$high_price_limit)[1]
    current_quantity_index = auction_quantity + 1
    distance = reserve_bid_index - current_quantity_index
    addIn = ifelse(current_reserve>=distance,distance,current_reserve)
    current_reserve = current_reserve - addIn
#    cat(sprintf("Price collar: current_reserve: %d\n", current_reserve))
    auction_quantity = auction_quantity+addIn
    auction_price = st.bids$bid[auction_quantity + 1 ]
  }
}
#
#  
    st.bids$id=1:nrow(st.bids)
    setkey(st.bids,id)
    st.bids$bid_accepted=0
    st.bids$tieKey=0
if (is.na(auction_price)) {
    auction_price = this_reserve_price
    num_purchased = min(auction_quantity,length(st.bids[!is.na(bid),bid]))
    auction_excess_supply=auction_quantity - num_purchased
# Unsold permits go into the reserve
    current_reserve = current_reserve + auction_excess_supply
#    cat(sprintf("NA price: auction_excess_supply: %d\n", auction_excess_supply))
# st.bids is sorted by value descending
    if (num_purchased > 0) st.bids[1:num_purchased,bid_accepted:=1]
  } else {
    # first, resolve ties
    st.bids[!is.na(bid),tieKey:=ifelse(bid==auction_price,1,0)]
    tieLength = as.numeric(st.bids[,sum(tieKey)])
    if (tieLength>1) {          #     & 
      st.bids[!is.na(bid),bid_accepted:=ifelse(bid>auction_price,1,0)]
      numNeeded = auction_quantity-nrow(st.bids[bid>auction_price])
      idArray = data.table(item_id=sample(st.bids[tieKey==1,item_id],numNeeded,replace=F),key="item_id")
      setkey(st.bids,item_id)
      st.bids[idArray,bid_accepted:=1]
    } else {
      st.bids[!is.na(bid),bid_accepted:=ifelse(bid>=auction_price,1,0)]
    }
    num_purchased = auction_quantity 
  }
#cat(sprintf("auction_price: %5.2f,  num_purchased: %d\n", auction_price,num_purchased))
# Production decision
# 
  st.bids = st.bids[order(subject_id,-value)]
  permits_available = num_purchased + t.bank
  t.subject$num_purchased = st.bids[,list(num_purchased = sum(bid_accepted)),by=subject_id][,num_purchased]
  t.subject$permits_available = t.subject[,ifelse(num_purchased + bank < 0,0,num_purchased+bank)]
#  t.bids$unit_used=0
  st.bids$unit_used = 0
  switch ( thisBehavior,
    "perfectly myopic" = {
#       t.bids[1:permits_available,unit_used:=unit]
        for (i in t.subject$subject_id) {
          if (t.subject$permits_available[i] %in% 1:session$num_values) {                       #This assumes no 'must serve' or other producing without permits to cover
            #temp = st.bids[subject_id==i]$unit[1:t.subject$permits_available[i]]
            st.bids[subject_id==i]$unit_used[1:t.subject$permits_available[i]] = st.bids[subject_id==i]$unit[1:t.subject$permits_available[i]]
          } else if (t.subject$permits_available[i]>session$num_values) {
            st.bids[subject_id==i][1:session$num_values,unit_used] = st.bids[subject_id==i][1:t.subject$permits_available[i],unit]
          }
        }
      },                                             # Fully myopic decision: use everything for which you have a permit
    "perfect foresight" = { 
#      t.bids[,unit_used:=unit*ifelse(value>=session$dynamic_efficient_price,1,0)]
      for (i in t.subject$subject_id) {
        if (t.subject$permits_available[i] %in% 1:session$num_values) {       #This assumes no must serve or other producing without permits to cover
          range = 1:t.subject$permits_available[i]
        } else if (t.subject$permits_available[i]>20) range = 1:session$num_values
        this_value = st.bids[subject_id==i][range,ifelse(unit_value>=session$dynamic_efficient_price,unit,0)]
        st.bids[subject_id==i][range,"unit_used"]=this_value
      }
    },                                             # Run only units where permits are worth more than the dynamic price
    stop("Please set a behavior.")  
  )  
      
  t.subject$permits_purchased = st.bids[,sum(bid_accepted),by=subject_id][,V1]
  t.subject$permits_used = st.bids[,sum(unit_used),by=subject_id][,V1]
  t.subject$bank = t.subject$bank + t.subject$permits_purchased - t.subject$permits_used
  t.subject[,penalty := ifelse(bank<0,bank*25,0)]
  t.subject$earnings = st.bids[,sum(value*unit_used - auction_price*bid_accepted ),by=subject_id][,V1] - t.subject$penalty
  t.subject$ending_bank = t.subject$bank
  t.subject$cash = t.subject$cash + t.subject$earnings
  
  this_surplus = st.bids[,sum(unit_used*(unit_value-session$dynamic_efficient_price))]
  num_permits_used = t.subject[,sum(permits_used)]
  earnings = t.subject[,sum(earnings)]

  t.bank = t.subject[,sum(ifelse(bank>=0,bank,0))]
#cat(sprintf("Permits available: %d,   Permits used: %d\n", permits_available,num_permits_used))
#cat(sprintf("t.bank: %d \n", t.bank))
#
  setkey(item,item_id)
  setkey(st.bids,item_id) #<- this is already done above
  item[st.bids,`:=`(bid_accepted=i.bid_accepted,
                   unit_used=i.unit_used, bid=i.bid)]
  price = auction_price
  period[t,`:=`(auction_price=price,
                auction_revenue = price*num_purchased,
                ending_total_bank=t.bank,
                ending_reserve = current_reserve,
                auction_quantity_sold = num_purchased,
                period_surplus = this_surplus,
                permits_used = num_permits_used
                )]
  setkey(subject,id);setkey(t.subject,id)
  subject[t.subject,`:=`(
                starting_cash = i.starting_cash,
                starting_bank = i.starting_bank,
                ending_cash = i.cash,
                earnings = i.earnings,
                ending_bank = i.bank,
                permits_purchased = i.permits_purchased,
                permits_used = i.permits_used
                )]
 }
  period[item[unit_used==1,list(production=sum(ifelse(unit_intensity==1,1.0,0.5))),by=period_num],production:=i.production]
  period[item[dynamic_producing_unit==1,list(dyn_eff_production=sum(ifelse(unit_intensity==1,1.0,0.5))),by=period_num],dyn_eff_production:=i.dyn_eff_production]
  session$actual_surplus = item[!is.na(unit_value),sum(unit_used*(unit_value-session$dynamic_efficient_price))]
  sessionOutput <- dbConnect("PostgreSQL",user="wms5f", password="manx(0)Rose",dbname="eu_experiments", host="localhost")
  dbWriteTable(sessionOutput,"sim_period",period,append=T,row.names=F)
  dbWriteTable(sessionOutput,"sim_session",session,append=T,row.names=F)
  dbWriteTable(sessionOutput,"sim_item",item,append=T,row.names=F)
  dbDisconnect(sessionOutput)

 }

# production = item[unit_used==1,list(production=sum(ifelse(unit_intensity==1,1.0,0.5))),by=period_num]
# dynamic_production = item[dynamic_producing_unit==1,list(dynamic_production=sum(ifelse(unit_intensity==1,1.0,0.5))),by=period_num]
# session$staticSurplus = item[unit_used==1,sum(value-session$dynamic_efficient_price)]

