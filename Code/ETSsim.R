#con1 <- dbConnect(MySQL(),user="wms5f", password="manx(0)Rose",dbname="Experiments", host="econ.ccps.virginia.edu")
#script  <- paste("show tables")
#test = dbGetQuery(con1, script)
require(taRifx)    # For 'shift' function
rm(list = ls(all = TRUE)) 
#i = 1
# set up session variables
sessionData = fread("simValues.csv")
numSessions = nrow(sessionData)
# During testing, truncate the output tables before proceeding
con <- dbConnect(MySQL(),user="wms5f", password="manx(0)Rose",dbname="test", host="localhost")
dbGetQuery(con, "DELETE FROM item;")
dbGetQuery(con, "DELETE FROM session;")
dbGetQuery(con, "DELETE FROM period;")
dbDisconnect(con)
# 
# Main session loop
for (sessionNum in sessionData$id) {                #sessionData$id    for all rows in the spreadsheet
#sessionNum=1
#
session = sessionData[sessionNum]
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
  item[period,value:=unit*(outputPrice/unitIntensity-cost)]
  setkey(item,periodNum,subjectID,value)
# Assign unit numbers from high to low, since sorting of values is from low to high. So unit 1 has the highest value, etc.
  item[unit==1 & unitIntensity==lowUserIntensity,unitNum:=rep(lowUserCapacity:1,times=(numLowUsers*numPeriods))]
  item[unit==1 & unitIntensity==highUserIntensity,unitNum:=rep(highUserCapacity:1,each=2,times=(numHighUsers*numPeriods))]
  item[,unitUsed:=0]
# Set up subject data table [numSubjects*numPeriods,]
  subject = data.table(id=1:(numSubjects*numPeriods))
  subject[,`:=`(subjectID=subjectIDs,
                periodNum=rep(1:numPeriods,each=numSubjects),
                startingBank=0, startingCash=0, earnings=0, endingCash=0,
                endingBank = 0, startingDeficit=0,endingDeficit=0,permitsUsed=0
          )]
  subject[periodNum==1,`:=`(startingBank=c(rep(lowUserInitBank,times=numLowUsers),rep(highUserInitBank,times=numHighUsers)),
                           startingCash=c(rep(lowUserInitCapital,times=numLowUsers),rep(highUserInitCapital,times=numHighUsers))
                           )]
#
  item = item[order(-value)]
  session$dynamicEfficientPrice = item[totalCap+initialBank+1,value]
  sessionDynamicValue = session$dynamicEfficientPrice
  item$dynamicProducingUnit=0
  item[1:(totalCap+initialBank),dynamicProducingUnit:=1]
  session$efficientSurplus = item[dynamicProducingUnit==1,sum(value-sessionDynamicValue)]
detach(session)
behavior = session$behavior
#
#periods = sort(as.array(period[,.SD[,id]]))
#  t.bids = item[,list(id,value,unit,periodNum)][order(-value)]

for (t in period$periodNum) {     #period$periodNum
#t = 1
  setkey(item,periodNum)
  thisPeriod = period[t]
  t.cap = thisPeriod$cap
  if (t==1) {
    currentReserve = session$initialReserve
    t.bank = thisPeriod$startingTotalBank
  } else {
    currentReserve = period$endingReserve[t-1]
    t.bank = period$endingTotalBank[t-1]
    period[t]$startingTotalBank = t.bank
    period[t]$startingReserve = currentReserve
  }
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
#  print(reserveAdjustment)
  currentReserve = max(0,currentReserve - reserveAdjustment)
#cat(sprintf("currentReserve: %d, current bank: %d\n", currentReserve,t.bank))
auctionQuantity = t.cap +  reserveAdjustment

#  bankDemandShift = ifelse(t.bank>session$fullCapacity,session$fullCapacity,t.bank)   # in Myopic sessions, use bank ASAP
bankDemandShift = min(t.bank,session$fullCapacity-session$numSubjects)
#cat(sprintf("bankDemandShift: %d\n", bankDemandShift))

  t.bids = item[.(t),list(itemID,value,unit)]
  t.bids = t.bids[order(-value)]
  switch ( behavior,
    "perfectly myopic" = {minBidValue = 0},
    "perfect foresight" = {minBidValue = session$dynamicEfficientPrice},
    stop("Please set a behavior.")  
    )
# Allowances in the bank come off the top of allowance values for all behaviors (could change this...)
# Later, we can do this by bidder, but for now it is annonymous
  t.bids[,bidValue:=shift(t.bids[,value],bankDemandShift,wrap=F,pad=T)]
# If bidValue is NA or lower than the dyn eff price, set it equal to dyn eff price
  t.bids[,bidValue:=ifelse(is.na(bidValue) ,minBidValue,bidValue)]
  t.bids[,bidValue:=ifelse(bidValue < minBidValue ,minBidValue,bidValue)]
#
  switch ( behavior,
      "perfectly myopic" = { t.bids[,bid:=ifelse(unit==1,bidValue,NA)] },     # In this simulation, there is no banking, so bid only on unit values
      "perfect foresight" = { t.bids[,bid:=bidValue]  },                    # Bid values for all available bids
      stop("Please set a behavior.")  
  )  
  t.bids[,bid:=ifelse(bid>=thisReservePrice,bid,NA)]  # No bids less than the reserve price are allowed. NA is no bid

#  currentPermitValue = ifelse(is.na(t.bids$value[auctionQuantity + t.bank ]),0,t.bids$value[auctionQuantity])
#  setkey(t.bids,periodNum)
# Auction reconciliation
# This is where the upper price collar function goes
#cat(sprintf("auctionQuantity: %d\n", auctionQuantity))
auctionPrice = t.bids$bid[auctionQuantity + 1 ]
if (session$priceCap) {                  # If price collar upper price is set; reserve price is turned off by setting it to zero.
  if(!is.na(auctionPrice) & auctionPrice > session$highPriceLimit ) {
    reserveBidIndex = which(t.bids[,bid]<=session$highPriceLimit)[1]
    currentQuantityIndex = auctionQuantity + 1
    distance = reserveBidIndex - currentQuantityIndex
    addIn = ifelse(currentReserve>=distance,distance,currentReserve)
    currentReserve = currentReserve - addIn
#    cat(sprintf("Price collar: currentReserve: %d\n", currentReserve))
    auctionPrice = t.bids$bid[auctionQuantity + 1 + addIn]
  }
}
#
  setkey(t.bids,itemID)
  t.bids$bidAccepted=0
  if (is.na(auctionPrice)) {
    auctionPrice = thisReservePrice
    numPurchased = min(auctionQuantity,length(t.bids[!is.na(bid),bid]))
    auctionExcessSupply=auctionQuantity - numPurchased
# Unsold permits go into the reserve
    currentReserve = currentReserve + auctionExcessSupply
#    cat(sprintf("NA price: auctionExcessSupply: %d\n", auctionExcessSupply))
# t.bids is sorted by value descending
    t.bids[1:numPurchased,bidAccepted:=1]
  } else {
    # first, resolve ties
    t.bids[,tieKey:=ifelse(value==auctionPrice,1,0)]
    tieLength = as.numeric(t.bids[,sum(tieKey)])
    if (tieLength>1 & t.bids$value[auctionQuantity ] == auctionPrice) {
      t.bids[,bidAccepted:=ifelse(bid>auctionPrice,1,0)]
      numNeeded = auctionQuantity - length(t.bids[bid>auctionPrice])
      idArray = data.table(id=sample(t.bids[tieKey==1,itemID],numNeeded,replace=F),key="id")
      t.bids[idArray,bidAccepted:=1]
    } else {
      t.bids[,bidAccepted:=ifelse(bid>=auctionPrice,1,0)]
    }
    numPurchased = auctionQuantity 
  }
#cat(sprintf("auctionPrice: %5.2f,  numPurchased: %d\n", auctionPrice,numPurchased))
  period$auctionRevenue = auctionPrice*numPurchased
# Production decision
  t.bids = t.bids[order(-value)]
  permitsAvailable = numPurchased + t.bank
  t.bids$unitUsed=0
  switch ( behavior,
    "perfectly myopic" = {t.bids[1:permitsAvailable,unitUsed:=unit]},     # Fully myopic decision: use everything for which you have a permit
    "perfect foresight" = { t.bids[,unitUsed:=unit*ifelse(value>=session$dynamicEfficientPrice,1,0)]  }, # Run only units where permits are worth more than the dynamic price
    stop("Please set a behavior.")  
  )  
      
  thisSurplus = t.bids[,sum(unitUsed*value)]
  numPermitsUsed = t.bids[,sum(unitUsed)]
  t.bank = max(0,permitsAvailable - numPermitsUsed)
#cat(sprintf("Permits available: %d,   Permits used: %d\n", permitsAvailable,numPermitsUsed))
#cat(sprintf("t.bank: %d \n", t.bank))
#
#  t.bids[,`:=`(unit=NULL,value=NULL,periodNum=NULL)]
  setkey(item,itemID)
  setkey(t.bids,itemID) #<- this is already done above
  item[t.bids,`:=`(bidAccepted=i.bidAccepted,unitUsed=i.unitUsed)]
  price = auctionPrice
  period[t,`:=`(auctionPrice=price,
                endingTotalBank=t.bank,
                endingReserve = currentReserve,
                auctionQuantitySold = numPurchased,
                periodSurplus = thisSurplus,
                permitsUsed = numPermitsUsed
                )]
}
period[item[unitUsed==1,list(production=sum(ifelse(unitIntensity==1,1.0,0.5))),by=periodNum],production:=i.production]
period[item[dynamicProducingUnit==1,list(dynEffProduction=sum(ifelse(unitIntensity==1,1.0,0.5))),by=periodNum],dynEffProduction:=i.dynEffProduction]
sessionOutput <- dbConnect(MySQL(),user="wms5f", password="manx(0)Rose",dbname="test", host="localhost")
print(sessionNum)
dbWriteTable(sessionOutput,"period",period,append=T,row.names=F)
dbWriteTable(sessionOutput,"session",session,append=T,row.names=F)
dbWriteTable(sessionOutput,"item",item,append=T,row.names=F)
dbDisconnect(sessionOutput)
}
# production = item[unitUsed==1,list(production=sum(ifelse(unitIntensity==1,1.0,0.5))),by=periodNum]
# dynamicProduction = item[dynamicProducingUnit==1,list(dynamicProduction=sum(ifelse(unitIntensity==1,1.0,0.5))),by=periodNum]
# session$staticSurplus = item[unitUsed==1,sum(value-session$dynamicEfficientPrice)]

