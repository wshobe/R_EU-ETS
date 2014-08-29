
require(taRifx)    # For 'shift' function
rm(list = ls(all = TRUE)) 
#i = 1
# set up session variables
sessionData = fread("simValues2.csv")
numSessions = nrow(sessionData)
# During testing, truncate the output tables before proceeding
con <- dbConnect(MySQL(),user="wms5f", password="manx(0)Rose",dbname="test", host="localhost")
dbGetQuery(con, "DELETE FROM item;")
dbGetQuery(con, "DELETE FROM session;")
dbGetQuery(con, "DELETE FROM period;")
dbDisconnect(con)
# 
# Main session loop
for (sessionNum in 16:18) {                #sessionData$id    for all rows in the spreadsheet
#sessionNum=18
print(sessionNum)
session = sessionData[id==sessionNum]
set.seed(1234)
session$numSubjects = session$numLowUsers + session$numHighUsers
attach(session)
  session$initialBank = lowUserInitBank*numLowUsers + highUserInitBank*numHighUsers
  session$fullCapacity = numLowUsers*lowUserCapacity*lowUserIntensity + numHighUsers*highUserCapacity*highUserIntensity
  session$sessionDataLength = numPeriods*numSubjects*numValues
  #
  period = data.table(periodNum=1:numPeriods,key="periodNum")
  period$outputPrice = rep(baseOutputPrice,numPeriods) + rbinom(numPeriods,1,highOutputPriceProbability)*10
  period$cap = seq(initialCapAmount,by=-capReduction,length.out=numPeriods)
  period$sessionID = sessionNum
detach(session)
period[,`:=`(
              auctionPrice = 0, auctionQuantitySold=0, auctionRevenue = 0
              )]
period[periodNum==1,`:=`(
              startingTotalBank=session$initialBank,
              startingReserve = session$initialReserve
              )]
session$numHighPricePeriods = period[outputPrice>session$baseOutputPrice,length(outputPrice)]
#
session$totalCap = period[,sum(cap)]
attach(session)
  subjectIDs = rep(1:numSubjects, times=numPeriods)
  item = data.table(itemID=array(1:sessionDataLength),key="itemID")
  item$subjectID = rep(subjectIDs, each=numValues)
  item$periodNum = rep(period$periodNum,each=numSubjects*numValues)
  item$sessionID = sessionNum
  lowUnits = rep(c(rep(1,lowUserCapacity*lowUserIntensity),rep(0,numValues-lowUserCapacity*lowUserIntensity)),times=numLowUsers)
  highUnits = rep(c(rep(1,highUserCapacity*highUserIntensity),rep(0,numValues-highUserCapacity*highUserIntensity)),times=numHighUsers)
  item$unit = rep(c(lowUnits,highUnits),times=numPeriods)  #for high users each entry is a half capacity unit
  item[,unitIntensity:=rep(c(rep(lowUserIntensity,numLowUsers*numValues),rep(highUserIntensity,numHighUsers*numValues)),times=numPeriods)]
# Assumes only 2 emission intensities
  item[unitIntensity==lowUserIntensity,rndCostFactor:=runif(numValues*numLowUsers*numPeriods)]
  item[unitIntensity==highUserIntensity,rndCostFactor:=rep(runif(numValues*numHighUsers*numPeriods),each=2,)]
# Costs are for production units only, set other values in 'for t' loop
  item[,cost:=round(unit*ifelse(unitIntensity==lowUserIntensity,(rndCostFactor*20+10),unit*(rndCostFactor*30)/unitIntensity),digits=2)]
  setkey(item,periodNum);setkey(period,periodNum)
  item[period,unitValue:=unit*(outputPrice/unitIntensity-cost)]
  setkey(item,periodNum,subjectID,unitValue)
# Assign unit numbers from high to low, since sorting of values is from low to high. So unit 1 has the highest value, etc.
  item[unit==1 & unitIntensity==lowUserIntensity,unitNum:=rep(lowUserCapacity:1,times=(numLowUsers*numPeriods))]
  item[unit==1 & unitIntensity==highUserIntensity,unitNum:=rep(highUserCapacity:1,each=2,times=(numHighUsers*numPeriods))]
  item[,unitUsed:=0]
# Set up subject data table [numSubjects*numPeriods,]
  subject = data.table(id = array(1:(numSubjects*numPeriods)),key="id") #One record per period.
  subject[,`:=`(subjectID=subjectIDs,
                periodNum=rep(1:numPeriods,each=numSubjects),
                startingBank=0, startingCash=0, earnings=0, endingCash=0,
                endingBank = 0,                                                 #       startingDeficit=0,endingDeficit=0, a deficit is a negative bank amouunt
                permitsPurchased=0, permitsUsed=0
          )]
  subject[periodNum==1,`:=`(startingBank=c(rep(lowUserInitBank,times=numLowUsers),rep(highUserInitBank,times=numHighUsers)),
                           startingCash=c(rep(lowUserInitCapital,times=numLowUsers),rep(highUserInitCapital,times=numHighUsers))
                           )]
  subject[,unitIntensity := rep(c(rep(lowUserIntensity,numLowUsers),rep(highUserIntensity,numHighUsers)),times=numPeriods)]
#
  item = item[order(-unitValue)]
  session$dynamicEfficientPrice = item[totalCap+initialBank+1,unitValue]
  sessionDynamicValue = session$dynamicEfficientPrice
  item$dynamicProducingUnit=0
  item[1:(totalCap+initialBank),dynamicProducingUnit:=1]
  session$efficientSurplus = item[dynamicProducingUnit==1,sum(unitValue-sessionDynamicValue)]
detach(session)
thisBehavior = session$behavior
#

for (t in period$periodNum) {     #period$periodNum
#t = 1
  thisPeriod = copy(period[t])
  t.subject = copy(subject[periodNum==t])
  t.cap = thisPeriod$cap
  if (t==1) {
    currentReserve = session$initialReserve
    t.bank = thisPeriod$startingTotalBank
    t.subject$bank = t.subject$startingBank
    t.subject$cash = t.subject$startingCash
#    subjectBanks = subject[periodNum==1,list(startingBank)]    #    Already done above
#    subjectsCash = subject[periodNum==1,list(startingCash)]
  } else {
    currentReserve = period$endingReserve[t-1]
    t.bank = period$endingTotalBank[t-1]
    period[t]$startingTotalBank = t.bank
    period[t]$startingReserve = currentReserve
    setkey(subject,periodNum)
    t.subject$bank = subject[periodNum==t-1,list(endingBank)]
    t.subject$startingBank = t.subject$bank
    t.subject$cash = subject[periodNum==t-1,list(endingCash)]
    t.subject$startingCash = t.subject$cash
#    st.bank = t.subjects[,list(subjectID,startingBank)]
  }
  t.subject[,lowBankBidKicker:=ifelse(startingBank<=unitIntensity*session$lowUserCapacity,1.1,1.0)]
  thisPeriod$startingReserve = currentReserve
  thisReservePrice = session$reservePrice       # Can be zero
  reserveAdjustment = 0    # No liquidity collar
  # The following code implements the liquidity collar
  if(session$liquidityCollar & t>2) {
    if (period$endingTotalBank[t-2]<session$triggerQuantityLowerBound ) {
      #Bank is small, pull permits out of the reserve
      reserveAdjustment = min(currentReserve,session$triggerQuantityInjection)
    } else if (period$endingTotalBank[t-2]>session$triggerQuantityUpperBound) {
      #bank is too big, put permits into the reserve
      reserveAdjustment = -1*floor(period$endingTotalBank[t-2]*session$triggerQuantityReductionProportion)
    } 
  }
#  cat(sprintf("ReserveAdjustment: %d\n", reserveAdjustment))
  currentReserve = max(0,currentReserve - reserveAdjustment)
  auctionQuantity = t.cap +  reserveAdjustment

#cat(sprintf("bankDemandShift: %d\n", bankDemandShift))
  setkey(item,periodNum)
  st.bids = copy(item[.(t),list(itemID,unitValue,unit,subjectID,unitNum)])
#  t.bids = t.bids[order(-value)]
  st.bids = copy(st.bids[order(subjectID,-unitValue)])
  st.bids[,value:=unitValue]
#thisBehavior = "perfect foresight"
switch ( thisBehavior,
    "perfectly myopic" = {
                          minBidValue = 0
                          t.subject[,lowBankBidKicker:=1.0]
                          st.bids[unit==0,value:=minBidValue]
                          },
    "perfect foresight" = {
                t.subject[,lowBankBidKicker:=ifelse(startingBank<=unitIntensity*session$lowUserCapacity,1.1,1.0)]
                
                minBidValue = session$dynamicEfficientPrice
                          st.bids[,value:=ifelse(unitValue<minBidValue,minBidValue,unitValue)]
                          # if
                          },
    stop("Please set a behavior.")  
    )
  st.bids = merge(st.bids,t.subject[,list(subjectID,startingBank,lowBankBidKicker)],by="subjectID")
# Allowances in the bank come off the top of allowance values for all behaviors (could change this...)
  s.bankDemandShift = t.subject[,ifelse(bank<0,0,bank)]
  for (i in 1:session$numSubjects) {
    ifelse(s.bankDemandShift[i] < session$numValues,
                st.bids[subjectID==i,bidValue:=shift(st.bids[subjectID==i,value],s.bankDemandShift[i],wrap=F,pad=T)],
                st.bids[subjectID==i,bidValue:=NA])
  }
  st.bids[,bidValue:=ifelse(is.na(bidValue) ,minBidValue*lowBankBidKicker,bidValue*lowBankBidKicker)]
#debug(warnings())
#warnings()
  st.bids[,bidValue:=ifelse(bidValue < minBidValue ,minBidValue,bidValue)]  
#  bankDemandShift = ifelse(t.bank>session$fullCapacity,session$fullCapacity,t.bank)   # in Myopic sessions, use bank ASAP
#  bankDemandShift = min(t.bank,session$fullCapacity)  # Not sure this is needed. 
#  t.bids[,bidValue:=shift(t.bids[,value],bankDemandShift,wrap=F,pad=T)]
# If bidValue is NA or lower than the dyn eff price, set it equal to dyn eff price
#  t.bids[,bidValue:=ifelse(is.na(bidValue) ,minBidValue,bidValue)]
#  t.bids[,bidValue:=ifelse(bidValue < minBidValue ,minBidValue,bidValue)]
#
  switch ( thisBehavior,
      "perfectly myopic" = { #t.bids[,bid:=ifelse(unit==1,bidValue,NA)] 
                             st.bids[,bid:=ifelse(unit==1,bidValue,NA)]
                             },     # In this simulation, there is no banking, so bid only on unit values
      "perfect foresight" = { #t.bids[,bid:=bidValue]  
                              st.bids[,bid:=bidValue]
                              },                    # Bid values for all 'numValues' available bids
      stop("Please set a behavior.")  
  )  
#  t.bids[,bid:=ifelse(bid>=thisReservePrice,bid,NA)]  # No bids less than the reserve price are allowed. NA is no bid
  st.bids[,bid:=ifelse(bid>=thisReservePrice,bid,NA)]
#  currentPermitValue = ifelse(is.na(t.bids$value[auctionQuantity + t.bank ]),0,t.bids$value[auctionQuantity])
#  setkey(t.bids,periodNum)
# Auction reconciliation
# This is where the upper price collar function goes
#cat(sprintf("auctionQuantity: %d\n", auctionQuantity))
st.bids = st.bids[order(-bid)]
#auctionPrice = t.bids$bid[auctionQuantity + 1 ]
auctionPrice =st.bids[order(-bid)][auctionQuantity + 1 ,bid]   # !!!!
t.bids = copy(st.bids)         # !!!!
if (session$priceCap) {                  # If price collar upper price is set; reserve price is turned off by setting it to zero.
  if(!is.na(auctionPrice) & auctionPrice > session$highPriceLimit ) {
    reserveBidIndex = which(t.bids[,bid]<=session$highPriceLimit)[1]
    currentQuantityIndex = auctionQuantity + 1
    distance = reserveBidIndex - currentQuantityIndex
    addIn = ifelse(currentReserve>=distance,distance,currentReserve)
    currentReserve = currentReserve - addIn
#    cat(sprintf("Price collar: currentReserve: %d\n", currentReserve))
    auctionQuantity = auctionQuantity+addIn
    auctionPrice = t.bids$bid[auctionQuantity + 1 ]
  }
}
#
#  
    t.bids$id=1:nrow(t.bids)
    setkey(t.bids,id)
    t.bids$bidAccepted=0
    t.bids$tieKey=0
if (is.na(auctionPrice)) {
    auctionPrice = thisReservePrice
    numPurchased = min(auctionQuantity,length(t.bids[!is.na(bid),bid]))
    auctionExcessSupply=auctionQuantity - numPurchased
# Unsold permits go into the reserve
    currentReserve = currentReserve + auctionExcessSupply
#    cat(sprintf("NA price: auctionExcessSupply: %d\n", auctionExcessSupply))
# t.bids is sorted by value descending
#    t.bids[,bidAccepted:=0]
    if (numPurchased > 0) t.bids[1:numPurchased,bidAccepted:=1]
  } else {
    # first, resolve ties
    t.bids[!is.na(bid),tieKey:=ifelse(bid==auctionPrice,1,0)]
    tieLength = as.numeric(t.bids[,sum(tieKey)])
    if (tieLength>1) {          #     & 
      t.bids[!is.na(bid),bidAccepted:=ifelse(bid>auctionPrice,1,0)]
      numNeeded = auctionQuantity-nrow(t.bids[bid>auctionPrice])
      idArray = data.table(itemID=sample(t.bids[tieKey==1,itemID],numNeeded,replace=F),key="itemID")
      setkey(t.bids,itemID)
      t.bids[idArray,bidAccepted:=1]
    } else {
      t.bids[!is.na(bid),bidAccepted:=ifelse(bid>=auctionPrice,1,0)]
    }
    numPurchased = auctionQuantity 
  }
#cat(sprintf("auctionPrice: %5.2f,  numPurchased: %d\n", auctionPrice,numPurchased))
# Production decision
  st.bids = copy(t.bids)             #!!!!!!!!!
# 
#  t.bids = t.bids[order(-value)]
  st.bids = st.bids[order(subjectID,-value)]
  permitsAvailable = numPurchased + t.bank
  t.subject$numPurchased = st.bids[,list(numPurchased = sum(bidAccepted)),by=subjectID][,numPurchased]
  t.subject$permitsAvailable = t.subject[,ifelse(numPurchased + bank < 0,0,numPurchased+bank)]
#  t.bids$unitUsed=0
  st.bids$unitUsed = 0
  switch ( thisBehavior,
    "perfectly myopic" = {
#       t.bids[1:permitsAvailable,unitUsed:=unit]
        for (i in t.subject$subjectID) {
          if (t.subject$permitsAvailable[i] %in% 1:session$numValues) {                       #This assumes no 'must serve' or other producing without permits to cover
            #temp = st.bids[subjectID==i]$unit[1:t.subject$permitsAvailable[i]]
            st.bids[subjectID==i]$unitUsed[1:t.subject$permitsAvailable[i]] = st.bids[subjectID==i]$unit[1:t.subject$permitsAvailable[i]]
          } else if (t.subject$permitsAvailable[i]>session$numValues) {
            st.bids[subjectID==i][1:session$numValues,unitUsed] = st.bids[subjectID==i][1:t.subject$permitsAvailable[i],unit]
          }
        }
      },                                             # Fully myopic decision: use everything for which you have a permit
    "perfect foresight" = { 
#      t.bids[,unitUsed:=unit*ifelse(value>=session$dynamicEfficientPrice,1,0)]
      for (i in t.subject$subjectID) {
        if (t.subject$permitsAvailable[i] %in% 1:session$numValues) {       #This assumes no must serve or other producing without permits to cover
          range = 1:t.subject$permitsAvailable[i]
        } else if (t.subject$permitsAvailable[i]>20) range = 1:session$numValues
        thisValue = st.bids[subjectID==i][range,ifelse(unitValue>=session$dynamicEfficientPrice,unit,0)]
        st.bids[subjectID==i][range,"unitUsed"]=thisValue
      }
    },                                             # Run only units where permits are worth more than the dynamic price
    stop("Please set a behavior.")  
  )  
      
  t.subject$permitsPurchased = st.bids[,sum(bidAccepted),by=subjectID][,V1]
  t.subject$permitsUsed = st.bids[,sum(unitUsed),by=subjectID][,V1]
  t.subject$bank = t.subject$bank + t.subject$permitsPurchased - t.subject$permitsUsed
  t.subject[,penalty := ifelse(bank<0,bank*25,0)]
  t.subject$earnings = st.bids[,sum(value*unitUsed - auctionPrice*bidAccepted ),by=subjectID][,V1] - t.subject$penalty
  t.subject$endingBank = t.subject$bank
  t.subject$cash = t.subject$cash + t.subject$earnings
  
  thisSurplus = st.bids[,sum(unitUsed*(unitValue-session$dynamicEfficientPrice))]
  numPermitsUsed = t.subject[,sum(permitsUsed)]
  earnings = t.subject[,sum(earnings)]

  t.bank = t.subject[,sum(ifelse(bank>=0,bank,0))]
#cat(sprintf("Permits available: %d,   Permits used: %d\n", permitsAvailable,numPermitsUsed))
#cat(sprintf("t.bank: %d \n", t.bank))
#
#  t.bids[,`:=`(unit=NULL,value=NULL,periodNum=NULL)]
  setkey(item,itemID)
  setkey(st.bids,itemID) #<- this is already done above
  item[st.bids,`:=`(bidAccepted=i.bidAccepted,
                   unitUsed=i.unitUsed, bid=i.bid)]
  price = auctionPrice
  period[t,`:=`(auctionPrice=price,
                auctionRevenue = price*numPurchased,
                endingTotalBank=t.bank,
                endingReserve = currentReserve,
                auctionQuantitySold = numPurchased,
                periodSurplus = thisSurplus,
                permitsUsed = numPermitsUsed
                )]
  setkey(subject,id);setkey(t.subject,id)
  subject[t.subject,`:=`(
                startingCash = i.startingCash,
                startingBank = i.startingBank,
                endingCash = i.cash,
                earnings = i.earnings,
                endingBank = i.bank,
                permitsPurchased = i.permitsPurchased,
                permitsUsed = i.permitsUsed
                )]
 }
  period[item[unitUsed==1,list(production=sum(ifelse(unitIntensity==1,1.0,0.5))),by=periodNum],production:=i.production]
  period[item[dynamicProducingUnit==1,list(dynEffProduction=sum(ifelse(unitIntensity==1,1.0,0.5))),by=periodNum],dynEffProduction:=i.dynEffProduction]
  session$actualSurplus = item[,sum(unitUsed*(unitValue-session$dynamicEfficientPrice))]
  sessionOutput <- dbConnect(MySQL(),user="wms5f", password="manx(0)Rose",dbname="test", host="localhost")
  dbWriteTable(sessionOutput,"period",period,append=T,row.names=F)
  dbWriteTable(sessionOutput,"session",session,append=T,row.names=F)
  dbWriteTable(sessionOutput,"item",item,append=T,row.names=F)
  dbDisconnect(sessionOutput)

 }

# production = item[unitUsed==1,list(production=sum(ifelse(unitIntensity==1,1.0,0.5))),by=periodNum]
# dynamicProduction = item[dynamicProducingUnit==1,list(dynamicProduction=sum(ifelse(unitIntensity==1,1.0,0.5))),by=periodNum]
# session$staticSurplus = item[unitUsed==1,sum(value-session$dynamicEfficientPrice)]

