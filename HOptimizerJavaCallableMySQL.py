# script that is called by the contextnet java system/program.
# it returns the optimal value of H, considering both the replicated 
# and basic subspace configurations.
# refer to contextnet modeling section and http://www14.in.tum.de/personen/raab/publ/balls.pdf
# for maximum balls and bins function

import math
#from scipy.optimize import minimize
from scipy.optimize import fsolve
import sys


#result = minimize(f, [1])
#print(result.x)
rho                             = 0.5
predRat                         = 0.25
#Yc                             = 1.0
#N                              = math.pow(10.0,5)
N                               = 32.0
# calculated by single node throughput, not very accurate estimation but let's go with that for now.
# specially if result size increases then estimation error might increase.
#CsByC                          = 0.005319149
#CsByC                          = 0.009713526
CuByC                           = 1.0/2700.0
CiByC                           = 1.0/2100.0
CdByC                           = 1.0/2400.0
CgByC                           = 1.0/14800.0
ChByC                           = 1.0/8220.0
#CiByC                          = 0.00071537
B                               = 20.0
Aavg                            = 4.0
NumGuids                        = 10000.0
CsByC                           = {}

#CsByC[1000]                     = 1.0/10156.0
#CsByC[2000]                     = 1.0/7932.0
#CsByC[3000]                     = 1.0/5731.0
#CsByC[4000]                     = 1.0/5234.0
#CsByC[5000]                     = 1.0/5108.0
#CsByC[6000]                     = 1.0/4541.0
#CsByC[7000]                     = 1.0/4033.0
#CsByC[8000]                     = 1.0/3644.0
#CsByC[9000]                     = 1.0/3247.0
#CsByC[10000]                    = 1.0/2956.0

CsByC[1000]                     = 1.0/3966.4365289681027
CsByC[2000]                     = 1.0/2969.6522295711543
CsByC[3000]                     = 1.0/2073.0960996514864
CsByC[4000]                     = 1.0/1596.8885783357027
CsByC[5000]                     = 1.0/1299.4137982864872
CsByC[6000]                     = 1.0/1092.8272750614385
CsByC[7000]                     = 1.0/942.81434571155
CsByC[8000]                     = 1.0/832.1314437733197
CsByC[9000]                     = 1.0/738.3749842278318
CsByC[10000]                    = 1.0/671.9668211231644


# if 0 then trigger not enable
# 1 if enable
triggerEnable                   = 0
CtByC                           = 0.000002838
CminByC                         = 0.000313117
QueryResidenceTime              = 30.0
disableOptimizer                = False
inputH                          = 5.0
inputConfigType                 = 1

BASIC_SUBSPACE_CONFIG           = 1
REPLICATED_SUBSPACE_CONFIG      = 2

MAXIMUM_H_VALUE                 = 10

# keys in the returned dictionary
functionValKey                  = 'funcValKey'
optimalHKey                     = 'optimalHKey'

#
# for calculating expected number of nodes a query goes to.
YByDArray = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]

def YbyDAverg(Aavg):
    sum = 0.0
    for i in YByDArray:
        for j in YByDArray:
            for k in YByDArray:
                sum = sum + i*j*k
    
    sum = sum/math.pow(10,Aavg-1)
    print "sum"+str(sum)
    return sum


def dAlphsFunc(x, c):
    if (x <= 0.0):
        return -1.0
    print "dAlphsFunc "+str(x) +" "+str(c)
    return 1.0 + x * (math.log(c) - math.log(x) + 1.0) - c

def solveDAlpha(c):
    data = (c)
    res = fsolve(dAlphsFunc, 1.0, args=data)
    print "solveDAlpha res.x "+str(res)
    for i in range(len(res)):
        if(res[i] > c):
            return res[i]
    # approximate it as c as paper says one root should be greater than c, i
    # if solver fails to find the root in 100 iterations
    return c
    

def calculateOverlapingNodesForSearch(numNodesForSubspace, currH):  
        expectedNumNodes = 0.0
        # calculating expected value of the first term
        for ybdi in YByDArray:
            expectedNumNodes = expectedNumNodes + \
                ( math.ceil(ybdi * math.pow(numNodesForSubspace, (1.0/currH)) ) )
        prob = 1.0/(1.0*len(YByDArray))
        expectedNumNodes = expectedNumNodes * prob
        
        print "Overlap for a predicate numNodesForSubspace "+str(numNodesForSubspace)+" currH "+str(currH)+" expectedNumOverlapNodes "+str(expectedNumNodes) 
        return expectedNumNodes
    
def calculateOverlapingNodesForSearchWithRatio(numNodesForSubspace, currH, yByDRatio):
        expectedNumNodes = \
                ( math.ceil(yByDRatio * math.pow(numNodesForSubspace, (1.0/currH)) ) )
        print "Overlap for a predicate numNodesForSubspace "+str(numNodesForSubspace)+" currH "+str(currH)+" expectedNumOverlapNodes "+str(expectedNumNodes) 
        
        #return expectedNumNodes
        return 1.5
        
    
    
    # calculates number fo ndoes for single subspace trigger
def calculateOverlapingNodesForTrigger(numNodesForSubspace, currH):
        expectedNumNodes = 0.0
        # calculating expected value of the first term
        for ybdi in YByDArray:
            expectedNumNodes = expectedNumNodes + math.ceil(ybdi * (numNodesForSubspace/currH)  )
        prob = 1.0/(1.0*len(YByDArray))
        expectedNumNodes = expectedNumNodes * prob
        
        print "calculateOverlapingNodesForTrigger for numNodesForSubspace "+str(numNodesForSubspace)+" currH "+str(currH)+" expectedNumOverlapNodes "+str(expectedNumNodes) 
        return expectedNumNodes
    

def calculateExpectedNumNodesASearchGoesTo(numNodesForSubspace, currH, currM):  
        expectedNumNodes = 0.0
        # calculating expected value of the first term
        #for ybdi in YByDArray:
        #    expectedNumNodes = expectedNumNodes + \
        #        ( math.ceil(ybdi * math.pow(numNodesForSubspace, (1.0/currH)) ) )
        #prob = 1.0/(1.0*len(YByDArray))
        #expectedNumNodes = expectedNumNodes * prob
        
        #expectedNumNodes = calculateOverlapingNodesForSearch(numNodesForSubspace, currH)
        expectedNumNodes = \
               calculateOverlapingNodesForSearchWithRatio(numNodesForSubspace, currH, predRat)
        
        expectedNumNodes = math.pow(expectedNumNodes, currM)
        mByH = (currM * 1.0)/(currH * 1.0)
        
        # calculating expected value of the second term
        expectedNumNodes = math.ceil(expectedNumNodes * math.pow(numNodesForSubspace, 1.0-mByH))
        
        print "numNodesForSubspace "+str(numNodesForSubspace)+" currH "+str(currH)+" currM "+str(currM)+" expectedNumSearchNodes "+str(expectedNumNodes) 
        return expectedNumNodes


def calcluateExpectedNumNodesAnUpdateGoesTo(numNodesForSubspace, currH):
    currP = math.pow(numNodesForSubspace, 1.0/currH)
    oneByP = 1.0/currP
    numNodesUpd = oneByP*1.0 + (1.0-oneByP)*2.0
    return numNodesUpd


def maxBallsFun(currH, Aavg, B):
    # optimizer sometimes sends negative values
    if(currH < 0.0):
        return 0.0
    
    m = Aavg
    n = math.ceil(B/currH)
    print "currH "+str(currH)+" m "+ str(m)+" n "+str(n)+" (n*math.log(n)) "+str((n*math.log(n)))
    alpha = 1.0
    
    if ( n== 1.0):
        return Aavg
    # all terms which can go negative are made zero
    # needed for the maths to hold
    if ( (m > 0.0) and (m < (n*math.log(n)) ) ):
        logNlogNByM = math.log( (n * math.log(n))/m )
        secondTerm = math.log(logNlogNByM)/logNlogNByM
        if(secondTerm < 0.0):
            secondTerm = 0.0
        retValue = (math.log(n)/logNlogNByM)*( 1.0+alpha * (secondTerm) )
        if(math.floor((n*math.log(n))/m) == 1.0):
            c = n*math.log(n)/m
            dAlpha = solveDAlpha(c)
            #dAlpha = 1.0
            retValue = (dAlpha -1.0 + alpha)*math.log(n)
        print "returned case 1 "+str(retValue)
        
        #return math.floor(retValue)
        return retValue
    elif ( (m >= n*math.log(n)) and m < n*math.pow(math.log(n),2.0) ):
        c = m/( n*math.log(n) )
        dAlpha = solveDAlpha(c)
        #dAlpha = 1.0
        retValue = (dAlpha -1.0 + alpha)*math.log(n)
        print "returned case 2 "+str(retValue)
        #return math.floor(retValue)
        return retValue
    
    elif ( (m >= n*(math.pow(math.log(n),2.0)) ) and (m <= n*(math.pow(math.log(n),3.0)) ) ):
        retValue = (m/n) + alpha*(math.sqrt(2.0*(m/n)*math.log(n)))
        print "returned case 3 "+str(retValue)
        #return math.floor(retValue)
        return retValue
    elif ( m > n*(math.pow(math.log(n),3.0)) ):
        secondTerm = ( math.log(math.log(n))/(2.0*alpha*math.log(n)))
        if(secondTerm < 0.0):
            secondTerm = 0.0
        
        retValue = (m/n) + math.sqrt( (2.0*(m/n)*math.log(n))*(1.0-  secondTerm) ) 
        print "returned case 4 "+str(retValue)
        #return math.floor(retValue)
        return retValue
    
    print "No case matching. Not possible "
    return (Aavg*currH)/B

# BASIC_SUBSPACE_CONFIG           = 1
# REPLICATED_SUBSPACE_CONFIG      = 2
# returns either NH/B nodes or sqrt(N) if sqrt(N) > B/H
def getNumNodesForASubspace(B, currH, N, configType):
    if( configType == BASIC_SUBSPACE_CONFIG ):
        return (N * currH)/B
    elif( configType == REPLICATED_SUBSPACE_CONFIG ):
        # no replication case
        if( (B/currH) > math.sqrt(N) ):
            print "\n B = "+str(B)+" currH "+str(currH)+" N= "+str(N)+" choosing basic config \n"
            return (N * currH)/B
        else:
            print "\n B = "+str(B)+" currH "+str(currH)+" N= "+str(N)+" choosing replicated config \n"
            return math.ceil(math.sqrt(N))
    
    
# equation becomes quadritic in case of active triggers.
def solveThroughputQuadriticEq(H, rho, N, CsByC, B, CuByC, Aavg, configType, CtByC, CminByC):
    currX= maxBallsFun(H, Aavg, B)
    print "currX "+str( currX ) +" currH "+str( H )
    if ( currX <= 0.0 ):
        currX = (Aavg*H)/B
        
    numNodesSubspace = getNumNodesForASubspace(B, H, N, configType)
    
    numNodesSearch   = calculateExpectedNumNodesASearchGoesTo(numNodesSubspace, H, currX)
    numNodesUpdate   = calcluateExpectedNumNodesAnUpdateGoesTo(numNodesSubspace, H)
        
    numTotalSubspsaces = N/numNodesSubspace
    
    numNodesTrigger  = calculateOverlapingNodesForTrigger(numNodesSubspace, H)
    numPartitions    = math.ceil(numNodesSubspace/H)
    
    print "currH "+str(H)+" numNodesSearch "+str(numNodesSearch)+" numNodesTrigger "+str(numNodesTrigger)
    
    #numActiveQueriesOnNode = numNodesTrigger * ((Aq * Aavg)/(numPartitions*B))
    #triggerGuidsRead = CminByC + numActiveQueriesOnNode * math.pow(0.5, Aavg-1)*CtByC
    # assuming basic, will be inaccurate in replicated
    # only one subspace will have more than 1 node, others will be just 1
                                               
    # quadritic equation is of the form a*x^2 + b*x + c = 0
    # calculating a first.
    # might need to replace it with actual expcetation
    YByDAvg = YbyDAverg(Aavg)
    numUniqueSubspaces = numTotalSubspsaces/((B/H)) 
    #* math.pow(YByD, Aavg -1.0)
    
    numActiveQueriesCoeff = numNodesTrigger * ((rho * QueryResidenceTime * Aavg)/(numPartitions*B))* YByDAvg *CtByC
    
    currP = math.pow(numNodesSubspace, 1.0/H)
    if(currP < 1):
        currP = 1
    oneByP = 1.0/currP
    #updComp1 = oneByP*1.0*CuByC + (1.0-oneByP)*2.0*CiByC
    updComp1 = CuByC + (1.0-oneByP)*CiByC
    
    #totalUpdLoad = (1.0 + (numTotalSubspsaces - 1.0))*CuByC + updComp1
    totalUpdLoad = CuByC + numUniqueSubspaces*updComp1 + (numTotalSubspsaces-numUniqueSubspaces)*CuByC
    #totalUpdLoad = 1.0 + (numTotalSubspsaces - 1.0) + numNodesUpdate
    
    a = 2.0 * (1.0-rho) * numActiveQueriesCoeff
    b = rho*numNodesSearch*CsByC + (1.0-rho) * totalUpdLoad + rho * Aavg * numNodesTrigger * CiByC \
    + (1.0-rho) * 2.0 * numUniqueSubspaces * CminByC
    c = -N
    
    print "a "+str(a)+" b "+str(b) +" c "+str(c)
    # now get the roots.
    if(a != 0.0):
        x1 = ( -b + math.sqrt(math.pow(b,2.0) - 4.0 * a * c) )/(2.0 * a)
        x2 = ( -b - math.sqrt(math.pow(b,2.0) - 4.0 * a * c) )/(2.0 * a)
    else:
        x1 = -c/b
        x2 = -c/b
    
    maxT = -1.0
    print "roots with a "+str(a)+" b "+str(b)+" c "+str(c)+" x1 "+str(x1)+" x2 "+str(x2)
    if(x1 > x2):
        maxT = x1
    else:
        maxT = x2
    return maxT  
    
def getCsByCValue(numNodesSubspace):
    guidsPerNode = NumGuids/numNodesSubspace
    # this will be removed later on, right now we have only values for Cs(g), g>= 1000
    if(guidsPerNode < 1000.0):
        guidsPerNode = 1000.0
        
    if(guidsPerNode > 10000.0):
        guidsPerNode = 10000.0
        
    guidPerNodeInt = (int) (guidsPerNode/1000.0)
    guidPerNodeInt = guidPerNodeInt*1000
    return CsByC[guidPerNodeInt]

    
# equation becomes linear if there are no triggers.
def solveThroughputLinearEq(H, rho, N, CsByC, B, CuByC, Aavg, configType, CtByC, CminByC):
    currX= maxBallsFun(H, Aavg, B)
    
    if ( currX <= 0.0 ):
        currX = (Aavg*H)/B
    
    currX = math.ceil(currX)
    print "currX "+str(currX)
    numNodesSubspace = getNumNodesForASubspace(B, H, N, configType)
    numNodesSearch = calculateExpectedNumNodesASearchGoesTo(numNodesSubspace, H, currX)
    #numNodesUpdate = calcluateExpectedNumNodesAnUpdateGoesTo(numNodesSubspace, H)
    
    #currP = math.pow(numNodesSubspace, 1.0/H)
    #if(currP < 1):
    #    currP = 1
        
    #oneByP = 1.0/currP
    oneByNumNodes = 1.0/numNodesSubspace
    if(oneByNumNodes > 1.0):
        oneByNumNodes = 1.0
    
    updComp1 = oneByNumNodes*1.0*CuByC + (1.0-oneByNumNodes)*(CiByC+CdByC)

    numTotalSubspsaces = 1.0*(N/numNodesSubspace)
    numUniqueSubspaces = numTotalSubspsaces/((B/H)) 
    
    # assuming basic, will be inaccurate in replicated
    # only one subspace will have more than 1 node, others will be just 1
    
    totalUpdLoad = CgByC + CuByC + ((2.0*numTotalSubspsaces)*ChByC)+ numUniqueSubspaces*updComp1 + (numTotalSubspsaces-numUniqueSubspaces)*CuByC
    
    csByCForCase = getCsByCValue(numNodesSubspace)
    totalSearchLoad = ChByC + (numNodesSearch*csByCForCase)
    #totalUpdLoad = (1.0 + (numTotalSubspsaces - 1.0))*CuByC + updComp1
    print "totalUpdLoad "+str(totalUpdLoad)+" currH "+str(H)+" numNodesSearch "+str(numNodesSearch)
    return N/(rho*totalSearchLoad + (1.0-rho) * totalUpdLoad )
    #return N/(rho*numNodesSearch*CsByC + (1.0-rho) * totalUpdLoad * CuByC)
    
    
# loops through all H values to check for optimal value of H
def loopOptimizer(rho, N, CsByC, B, CuByC, Aavg, configType, triggerEnable, CtByC, CminByC):
    valueDict = {}
    optimalH  = -1.0
    maxValue  = -1.0
    currH     = 2.0
    while( currH <= MAXIMUM_H_VALUE and currH <= B ):
        currT = -1.0
        if(triggerEnable == 0):
            currT = solveThroughputLinearEq(currH, rho, N, CsByC, B, CuByC, Aavg, configType, CtByC, CminByC)
        else:
            print "loopOptimizer currH "+str(currH)
            currT = solveThroughputQuadriticEq(currH, rho, N, CsByC, B, CuByC, Aavg, configType, CtByC, CminByC)
            
        valueDict[currH] = currT
        if( currH == 2.0 ):
            optimalH = currH
            maxValue = currT
        else:
            if(currT > maxValue):
                optimalH = currH
                maxValue = currT
        currH = currH + 1.0
    returnDict = {}
    #functionValKey                  = 'funcValKey'
    #optimalHKey                     = 'optimalHKey'
    returnDict[functionValKey] = maxValue
    returnDict[optimalHKey] = optimalH
    print "rho "+ str(rho)+" optimalH "+str(optimalH)+" maxValue "+str(maxValue)+"\n"
    return returnDict

    
if(len(sys.argv) >= 13):
    rho              = float(sys.argv[1])
    N                = float(sys.argv[2])
    # calculated by single node throughput, not very accurate estimation but let's go with that for now.
    # specially if result size increases then estimation error might increase.
    CsByC            = float(sys.argv[3])
    CuByC            = float(sys.argv[4])
    B                = float(sys.argv[5])
    Aavg             = float(sys.argv[6])
    triggerEnable    = int(sys.argv[7])
    CtByC            = float(sys.argv[8])
    CminByC          = float(sys.argv[9])
    CiByC            = float(sys.argv[10])
    disableOptimizer = bool(sys.argv[11])
    inputH           = float(sys.argv[12])
    inputConfigType  = int(sys.argv[13])
    
print "rho "+str(rho)+" N "+str(N)+" CsByC "+str(CsByC)+" CuByC "\
+str(CuByC)+" B "+str(B)+" Aavg "+str(Aavg)+" triggerEnable "+str(triggerEnable)+" CtByC "+str(CtByC)

if(not disableOptimizer):
    basicResultDict = loopOptimizer(rho, N, CsByC, B, CuByC, Aavg, BASIC_SUBSPACE_CONFIG, triggerEnable, CtByC, CminByC)
    repResultDict   = loopOptimizer(rho, N, CsByC, B, CuByC, Aavg, REPLICATED_SUBSPACE_CONFIG, triggerEnable, CtByC, CminByC)
    
    # compare which scheme is better, replicated or basic configuration
    #functionValKey                  = 'funcValKey'
    #optimalHKey                     = 'optimalHKey'

    basicFuncValue  = basicResultDict[functionValKey]
    repFuncValue    = repResultDict[functionValKey]

    # basic config better
    if( basicFuncValue >= repFuncValue ):
        print "BASIC OPTIMIZATION RESULT H "+str(basicResultDict[optimalHKey])+" MAXVALUE "+str(basicFuncValue) \
        +" OTHERVAL "+str(repFuncValue)
    else:
        print "REPLICATED OPTIMIZATION RESULT H "+str(repResultDict[optimalHKey])+" MAXVALUE "+str(repFuncValue) \
        +" OTHERVAL "+str(basicFuncValue)
    
    print "###################\n\n\n\n"
    print "num trigger sub "+str(calculateOverlapingNodesForTrigger(2.0, 10.0))
    #hyperspaceHashingModel(10.0, rho, N, CsByC, B, CuByC, Aavg, BASIC_SUBSPACE_CONFIG, triggerEnable, CtByC, CminByC)
else:
    currT = -1.0
    if(triggerEnable == 0):
        currT = solveThroughputLinearEq(inputH, rho, N, CsByC, B, CuByC, Aavg, inputConfigType, CtByC, CminByC)
    else:
        print "disabled optimizer at work inputH "+str(inputH)
        currT = solveThroughputQuadriticEq(inputH, rho, N, CsByC, B, CuByC, Aavg, inputConfigType, CtByC, CminByC)
    
    if( inputConfigType == BASIC_SUBSPACE_CONFIG ):
        print "BASIC OPTIMIZATION RESULT H "+str(inputH)+" MAXVALUE "+str(currT) +" OTHERVAL "+str(currT)
    elif( inputConfigType == REPLICATED_SUBSPACE_CONFIG ):
        print "REPLICATED OPTIMIZATION RESULT H "+str(inputH)+" MAXVALUE "+str(currT)+" OTHERVAL "+str(currT)