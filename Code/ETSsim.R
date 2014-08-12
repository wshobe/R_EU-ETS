#con1 <- dbConnect(MySQL(),user="wms5f", password="manx(0)Rose",dbname="Experiments", host="econ.ccps.virginia.edu")
#script  <- paste("show tables")
#test = dbGetQuery(con1, script)

set.seed(1234)
rm(session)
i = 1
# set up session variables
session = data.table(id=i)
session$numValues = 20
session$numHighUsers = 3
session$numLowUsers = 3
session$numSubjects = session$numLowUsers + session$numHighUsers
session$highUserCapacity = 4
session$lowUserCapacity = 4
session$highUserIntensity = 2
session$lowUserIntensity = 1
session$highUserInitBank = 1
session$lowUserInitBank = 2
session$lowUserInitCapital = 50
session$highUserInitCapital = 100
session$numPeriods = 18
session$initialReserve = 12
session$initialCapAmount = 29
session$capReduction = 1
session$reservePrice = 7
session$highPriceLimit = 100
#session$liquidityCollar = T
session$triggerQuantityLowerBound = 18
session$triggerQuantityUpperBound = 40
session$triggerQuantityInjection = 6
session$triggerQuantityReductionProportion = 0.15
attach(session)
session$initialBank = lowUserInitBank*numLowUsers + highUserInitBank*numHighUsers
session$fullCapacity = numLowUsers*lowUserCapacity*lowUserIntensity + numHighUsers*highUserCapacity*highUserIntensity
session$sessionDataLength = numPeriods*numSubjects*numValues
detach(session)
#session
#
period = data.table(id=1:session$numPeriods)
period$outputPrice = rep(30,session$numPeriods) + rbinom(session$numPeriods,1,0.5)*10
period$cap = seq(session$initialCapAmount,by=-session$capReduction,length.out=session$numPeriods)
period$sessionID = session$id
period[,`:=`(
              auctionPrice = 0, auctionQuantitySold=0
              )]
period[id==1,`:=`(
              startingTotalBank=session$initialBank,
              startingReserve = session$initialReserve
              )]
session$numHighPricePeriods = period[outputPrice>30,length(.SD[,outputPrice])]
#
session$totalCap = period[,sum(cap)]
attach(session)
  subjectIDs = rep(1:numSubjects, times=numPeriods)
  item = data.table(id=array(1:sessionDataLength))
  item$subjectID = rep(subjectIDs, each=numValues)
  item$periodID = rep(period$id,each=numSubjects*numValues)
  lowUnits = rep(c(rep(1,lowUserCapacity*lowUserIntensity),rep(0,numValues-lowUserCapacity*lowUserIntensity)),times=numLowUsers)
  highUnits = rep(c(rep(1,highUserCapacity*highUserIntensity),rep(0,numValues-highUserCapacity*highUserIntensity)),times=numHighUsers)
  item$unit = rep(c(lowUnits,highUnits),times=numPeriods)  #for high users each entry is a half capacity unit
  item[,unitIntensity:=rep(c(rep(lowUserIntensity,numLowUsers*numValues),rep(highUserIntensity,numHighUsers*numValues)),times=numPeriods)]
# Assumes only 2 emission intensities
  item[unitIntensity==lowUserIntensity,rndCostFactor:=runif(numValues*numLowUsers*numPeriods)]
  item[unitIntensity==highUserIntensity,rndCostFactor:=rep(runif(numValues*numHighUsers*numPeriods),each=2,)]
# Costs are for production units only, set other values in 'for t' loop
  item[,cost:=round(unit*ifelse(unitIntensity==lowUserIntensity,(rndCostFactor*20+10),unit*(rndCostFactor*30)/unitIntensity),digits=2)]
  setkey(item,periodID);setkey(period,id)
  item[period,value:=unit*(outputPrice/unitIntensity-cost)]
  setkey(item,periodID,subjectID,value)
# Assign unit numbers from high to low, since sorting of values is from low to high. So unit 1 has the highest value, etc.
  item[unit==1 & unitIntensity==lowUserIntensity,unitNum:=rep(lowUserCapacity:1,times=(numLowUsers*numPeriods))]
  item[unit==1 & unitIntensity==highUserIntensity,unitNum:=rep(highUserCapacity:1,each=2,times=(numHighUsers*numPeriods))]
  item[,unitUsed:=0]
# Set up subject data table [numSubjects*numPeriods,]
  subject = data.table(id=1:(numSubjects*numPeriods))
  subject[,`:=`(subjectID=subjectIDs,
                periodID=rep(1:numPeriods,each=numSubjects),
                startingBank=0, startingCash=0, earnings=0, endingCash=0,
                endingBank = 0, startingDeficit=0,endingDeficit=0,permitsUsed=0
          )]
  subject[periodID==1,`:=`(startingBank=c(rep(lowUserInitBank,times=numLowUsers),rep(highUserInitBank,times=numHighUsers)),
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
#
#periods = sort(as.array(period[,.SD[,id]]))
#  t.bids = item[,list(id,value,unit,periodID)][order(-value)]

for (t in period$id) {     #period$id
#t = 1
  setkey(item,periodID)
  thisPeriod = period[t]
  t.cap = thisPeriod$cap
  if (t==1) {
    print(t)
    currentReserve = session$initialReserve
    t.bank = thisPeriod$startingTotalBank
  } else {
    print(t)
    currentReserve = period$endingReserve[t-1]
    t.bank = period$endingTotalBank[t-1]
    period[t]$startingTotalBank = t.bank
    period[t]$startingReserve = currentReserve
  }
  thisPeriod$startingReserve = currentReserve
  thisReservePrice = session$reservePrice       # Can be zero
  reserveAdjustment = 0    # No liquidity collar
  if(session$triggerQuantityInjection >0 & t>2) {
    if (period$endingTotalBank[t-2]<session$triggerQuantityLowerBound ) {
      #Bank is small, pull permits out of the reserve
      reserveAdjustment = min(currentReserve,session$triggerQuantityInjection)
    } else if (period$endingTotalBank[t-2]>session$triggerQuantityUpperBound) {
      #bank is too big, put permits into the reserve
      reserveAdjustment = -1*floor(period$endingTotalBank[t-2]*session$triggerQuantityReductionProportion)
    } 
  }
  cat(sprintf("ReserveAdjustment: %d\n", reserveAdjustment))
#  print(reserveAdjustment)
  currentReserve = max(0,currentReserve - reserveAdjustment)
cat(sprintf("currentReserve: %d, current bank: %d\n", currentReserve,t.bank))
auctionQuantity = t.cap +  reserveAdjustment

#  bankDemandShift = ifelse(t.bank>session$fullCapacity,session$fullCapacity,t.bank)   # in Myopic sessions, use bank ASAP
bankDemandShift = min(t.bank,session$fullCapacity-session$numSubjects)
cat(sprintf("bankDemandShift: %d\n", bankDemandShift))

t.bids = item[.(t),list(id,value,unit)]
  t.bids = t.bids[order(-value)]
#  minBidValue = session$dynamicEfficientPrice # Perfect foresight
  minBidValue = 0 # Myopic bids
require(taRifx)    # For 'shift' function
# Allowances in the bank come off the top of allowance values
# Later, we can do this by bidder, but for now it is annonymous
#print(t.bids[,value])
  t.bids[,bidValue:=shift(t.bids[,value],bankDemandShift,wrap=F,pad=T)]
#print(t.bids$bidValue)
# If bidValue is NA or lower than the dyn eff price, set it equal to dyn eff price
  t.bids[,bidValue:=ifelse(is.na(bidValue) ,minBidValue,bidValue)]
  t.bids[,bidValue:=ifelse(bidValue < minBidValue ,minBidValue,bidValue)]
#print(t.bids$bidValue)
  t.bids[,bid:=bidValue] 
#print(t.bids$bid)
  t.bids[,bid:=ifelse(unit==1,bid,NA)]  # In this simulation, there is no banking, so bid only on unit values
  t.bids[,bid:=ifelse(bid>=thisReservePrice,bid,NA)]  # No bids less than the reserve price are allowed. NA is no bid

#  currentPermitValue = ifelse(is.na(t.bids$value[auctionQuantity + t.bank ]),0,t.bids$value[auctionQuantity])
#  setkey(t.bids,periodID)
# Auction reconciliation
# This is where the upper price collar function goes
cat(sprintf("auctionQuantity: %d\n", auctionQuantity))
auctionPrice = t.bids$bid[auctionQuantity + 1 ]

if(!is.na(auctionPrice) & auctionPrice > session$highPriceLimit ) {
    reserveBidIndex = which(t.bids[,bid]<=session$highPriceLimit)[1]
    currentQuantityIndex = auctionQuantity + 1
    distance = reserveBidIndex - currentQuantityIndex
    addIn = ifelse(currentReserve>=distance,distance,currentReserve)
    currentReserve = currentReserve - addIn
    cat(sprintf("Price collar: currentReserve: %d\n", currentReserve))
    auctionPrice = t.bids$bid[auctionQuantity + 1 + addIn]
}
#
  setkey(t.bids,id)
  t.bids$bidAccepted=0
  if (is.na(auctionPrice)) {
    auctionPrice = thisReservePrice
    numPurchased = min(auctionQuantity,length(t.bids[!is.na(bid),bid]))
    auctionExcessSupply=auctionQuantity - numPurchased
# Unsold permits go into the reserve
    currentReserve = currentReserve + auctionExcessSupply
   cat(sprintf("NA price: auctionExcessSupply: %d\n", auctionExcessSupply))
# t.bids is sorted by value descending
    t.bids[1:numPurchased,bidAccepted:=1]
  } else {
    # first, resolve ties
    t.bids[,tieKey:=ifelse(value==auctionPrice,1,0)]
    tieLength = as.numeric(t.bids[,sum(tieKey)])
    if (tieLength>1 & t.bids$value[auctionQuantity ] == auctionPrice) {
      t.bids[,bidAccepted:=ifelse(bid>auctionPrice,1,0)]
      numNeeded = auctionQuantity - length(t.bids[bid>auctionPrice])
      idArray = data.table(id=sample(t.bids[tieKey==1,id],numNeeded,replace=F),key="id")
      t.bids[idArray,bidAccepted:=1]
    } else {
      t.bids[,bidAccepted:=ifelse(bid>=auctionPrice,1,0)]
    }
    numPurchased = auctionQuantity 
  }
cat(sprintf("auctionPrice: %5.2f,  numPurchased: %d\n", auctionPrice,numPurchased))
# Production decision
  t.bids = t.bids[order(-value)]
  permitsAvailable = numPurchased + t.bank
  t.bids$unitUsed=0
  t.bids[1:permitsAvailable,unitUsed:=unit]      # Fully myopic decision: use everything for which you have a permit
#  t.bids[,unitUsed:=unit*ifelse(value>=session$dynamicEfficientPrice,1,0)]   # Run only units where permits are worth more than the dynamic price
  thisSurplus = t.bids[,sum(unitUsed*value)]
  numPermitsUsed = t.bids[,sum(unitUsed)]
  t.bank = max(0,permitsAvailable - numPermitsUsed)
cat(sprintf("Permits available: %d,   Permits used: %d\n", permitsAvailable,numPermitsUsed))
cat(sprintf("t.bank: %d \n", t.bank))
#
#  t.bids[,`:=`(unit=NULL,value=NULL,periodID=NULL)]
  setkey(item,id)
  setkey(t.bids,id) #<- this is already done above
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
production = item[unitUsed==1,list(production=sum(ifelse(unitIntensity==1,1.0,0.5))),by=periodID]
dynamicProduction = item[dynamicProducingUnit==1,list(dynamicProduction=sum(ifelse(unitIntensity==1,1.0,0.5))),by=periodID]
session$staticSurplus = item[unitUsed==1,sum(value-session$dynamicEfficientPrice)]
