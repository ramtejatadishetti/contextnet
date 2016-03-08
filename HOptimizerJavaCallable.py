# script that is called by the contextnet java system/program.
# it returns the optimal value of H, considering both the replicated 
# and basic subspace configurations.
# refer to contextnet modelling section and http://www14.in.tum.de/personen/raab/publ/balls.pdf for maximum balls and bins function

import math
#from scipy.optimize import minimize
from scipy.optimize import fsolve
import sys


#result = minimize(f, [1])
#print(result.x)
rho                             = 0.0
#Yc                             = 1.0
N                               = 36.0
# calculated by single node throughput, not very accurate estimation but let's go with that for now.
# specially if result size increases then estimation error might increase.
CsByC                           = 0.005319149
CuByC                           = 0.001388889
B                               = 20.0
Aavg                            = 4.0

# if 0 then trigger not enable
# 1 if enable
triggerEnable                   = 1
CtByC                           = 0.000009028
Aq                              = 100*100*4
CminByC                         = 0.00063231

numAttrsInUpdate                = 1

BASIC_SUBSPACE_CONFIG           = 1
REPLICATED_SUBSPACE_CONFIG      = 2

MAXIMUM_H_VALUE                 = 10

# keys in the returned dictionary
functionValKey                  = 'funcValKey'
optimalHKey                     = 'optimalHKey'


#
# for calculating expected number of nodes a query goes to.
YByDArray = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]

#def solveDAlpha(c):
#    x = sympy.Symbol('x')
#    res = sympy.solve(1.0 + x * (sympy.log(c) - sympy.log(x) + 1) - c, x)
#    
#    for i in range(len(res)):
#        if(res[i] > c):
#            return res[i]
#    return -1

def dAlphsFunc(x, c):
    if (x <= 0):
        return -1
    print "dAlphsFunc "+str(x) +" "+str(c)
    return 1.0 + x * (math.log(c) - math.log(x) + 1) - c

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
        for ybdi in YByDArray:
            expectedNumNodes = expectedNumNodes + \
                ( math.ceil(ybdi * math.pow(numNodesForSubspace, (1.0/currH)) ) )
        prob = 1.0/(1.0*len(YByDArray))
        expectedNumNodes = expectedNumNodes * prob
        expectedNumNodes = math.pow(expectedNumNodes, currM)
        mByH = (currM * 1.0)/(currH * 1.0)
        
        # calculating expected value of the second term
        expectedNumNodes = expectedNumNodes * math.pow(numNodesForSubspace, 1-mByH)
        print "numNodesForSubspace "+str(numNodesForSubspace)+" currH "+str(currH)+" currM "+str(currM)+" expectedNumSearchNodes "+str(expectedNumNodes) 
        return expectedNumNodes

def calcluateExpectedNumNodesAnUpdateGoesTo(numNodesForSubspace, currH):
    currP = math.pow(numNodesForSubspace, 1.0/currH)
    oneByP = 1.0/currP
    numNodesUpd = oneByP*1 + (1-oneByP)*2
    return numNodesUpd
    
    
def maxBallsFun(currH, Aavg, B):
    # optimizer sometimes sends negative values
    if(currH < 0):
        return 0.0
    
    m = Aavg
    n = math.ceil(B/currH)
    print "currH "+str(currH)+" m "+ str(m)+" n "+str(n)+" (n*math.log(n)) "+str((n*math.log(n)))
    alpha = 1.0
    
    if ( n== 1.0):
        return Aavg
    # all terms which can go negative are made zero
    # needed for the maths to hold
    if ( (m > 0) and (m < (n*math.log(n)) ) ):
        logNlogNByM = math.log( (n * math.log(n))/m )
        secondTerm = math.log(logNlogNByM)/logNlogNByM
        if(secondTerm < 0):
            secondTerm = 0
        retValue = (math.log(n)/logNlogNByM)*( 1+alpha * (secondTerm) )
        if(math.floor((n*math.log(n))/m) == 1.0):
            c = n*math.log(n)/m
            dAlpha = solveDAlpha(c)
            #dAlpha = 1.0
            retValue = (dAlpha -1 + alpha)*math.log(n)
        print "returned case 1 "+str(retValue)
        
        #return math.floor(retValue)
        return retValue
    elif ( (m >= n*math.log(n)) and m < n*math.pow(math.log(n),2) ):
        c = m/( n*math.log(n) )
        dAlpha = solveDAlpha(c)
        #dAlpha = 1.0
        retValue = (dAlpha -1 + alpha)*math.log(n)
        print "returned case 2 "+str(retValue)
        #return math.floor(retValue)
        return retValue
    
    elif ( (m >= n*(math.pow(math.log(n),2)) ) and (m <= n*(math.pow(math.log(n),3)) ) ):
        retValue = (m/n) + alpha*(math.sqrt(2*(m/n)*math.log(n)))
        print "returned case 3 "+str(retValue)
        #return math.floor(retValue)
        return retValue
    elif ( m > n*(math.pow(math.log(n),3)) ):
        secondTerm = ( math.log(math.log(n))/(2*alpha*math.log(n)))
        if(secondTerm < 0):
            secondTerm = 0
        
        retValue = (m/n) + math.sqrt( (2*(m/n)*math.log(n))*(1-  secondTerm) ) 
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
    
def hyperspaceHashingModel(H, rho, N, CsByC, B, CuByC, Aavg, configType, triggerEnable, CtByC, Aq, CminByC):
    #H = round(H)
    currX= maxBallsFun(H, Aavg, B)
    #currX = Aavg
    print "currX "+str(currX) +" currH "+str(H)
    if ( (currX > 0) ):
        numNodesSubspace = getNumNodesForASubspace(B, H, N, configType)
        numNodesSearch = calculateExpectedNumNodesASearchGoesTo(numNodesSubspace, H, currX)
        numNodesUpdate = calcluateExpectedNumNodesAnUpdateGoesTo(numNodesSubspace, H)
        
        numTotalSubspsaces = N/numNodesSubspace
        # assuming basic, will be inaccurate in replicated
        # only one subspace will have more than 1 node, others will be jsut 1
        totalUpdLoad = numTotalSubspsaces - 1.0 + numNodesUpdate
        if(triggerEnable == 0):
            return rho*numNodesSearch*CsByC + (1-rho) * totalUpdLoad * CuByC
        else:
            basicSum = rho*numNodesSearch*CsByC + (1-rho) * totalUpdLoad * CuByC
            numNodesTrigger = calculateOverlapingNodesForTrigger(numNodesSubspace, H)
            numPartitions = math.ceil(numNodesSubspace/H)
            
            numActiveQueriesOnNode = numNodesTrigger * ((Aq * Aavg)/(numPartitions*B))
            
            triggerGuidsRead = CminByC + numActiveQueriesOnNode * math.pow(0.5, Aavg-1)*CtByC
            triggerSum = rho * Aavg * numNodesTrigger * CuByC + (1-rho) * 2 * numAttrsInUpdate * (numTotalSubspsaces/(B/H)) * triggerGuidsRead
            
            return basicSum + triggerSum
    else:
        currX = (Aavg*H)/B
        print "Not a good value "
        numNodesSubspace = getNumNodesForASubspace(B, H, N, configType)
        numNodesSearch = calculateExpectedNumNodesASearchGoesTo(numNodesSubspace, H, currX)
        numNodesUpdate = calcluateExpectedNumNodesAnUpdateGoesTo(numNodesForSubspace, H)
        numTotalSubspsaces = N/numNodesSubspace

        # assuming basic, will be inaccurate in replicated
        # only one subspace will have more than 1 node, others will be just 1
        totalUpdLoad = numTotalSubspsaces - 1.0 + numNodesUpdate
        
        if(triggerEnable == 0):
            return rho*numNodesSearch*CsByC + (1-rho) * totalUpdLoad * CuByC
        else:
            basicSum = rho*numNodesSearch*CsByC + (1-rho) * totalUpdLoad * CuByC
            numNodesTrigger = calculateOverlapingNodesForTrigger(numNodesSubspace, H)
            numPartitions = math.ceil(numNodesSubspace/H)
            
            numActiveQueriesOnNode = numNodesTrigger * ((Aq * Aavg)/(numPartitions*B))
            
            triggerGuidsRead = CminByC + numActiveQueriesOnNode * math.pow(0.5, Aavg-1)*CtByC
            triggerSum = rho * Aavg * numNodesTrigger * CuByC + (1-rho) * 2 * numAttrsInUpdate * (numTotalSubspsaces/(B/H)) * triggerGuidsRead
            
            return basicSum + triggerSum
        #return rho*numNodesSearch*CsByC + (1-rho) * totalUpdLoad * CuByC
    
# loops through all H values to check for optimal value of H
def loopOptimizer(rho, N, CsByC, B, CuByC, Aavg, configType, triggerEnable, CtByC, Aq, CminByC):
    valueDict = {}
    optimalH  = -1.0
    minValue  = -1.0
    currH     = 2.0
    while( currH <= MAXIMUM_H_VALUE ):
        currValue = hyperspaceHashingModel(currH, rho, N, CsByC, B, CuByC, Aavg, configType, triggerEnable, CtByC, Aq, CminByC)
        valueDict[currH] = currValue
        if( currH == 2.0 ):
            optimalH = currH
            minValue = currValue
        else:
            if(currValue < minValue):
                optimalH = currH
                minValue = currValue
        currH = currH + 1.0
    returnDict = {}
    #functionValKey                  = 'funcValKey'
    #optimalHKey                     = 'optimalHKey'
    returnDict[functionValKey] = minValue
    returnDict[optimalHKey] = optimalH
    print "rho "+ str(rho)+" optimalH "+str(optimalH)+" minValue "+str(minValue)+"\n"
    return returnDict
    
if(len(sys.argv) >= 10):
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
    Aq               = float(sys.argv[9])
    CminByC          = float(sys.argv[10])
    
print "rho "+str(rho)+" N "+str(N)+" CsByC "+str(CsByC)+" CuByC "+str(CuByC)+" B "+str(B)+" Aavg "+str(Aavg)+" triggerEnable "+str(triggerEnable)+" CtByC "+str(CtByC)+" Aq "+str(Aq)
# 1 for basic config, 2 replicated config
#configType       = float(sys.argv[7])

# BASIC_SUBSPACE_CONFIG           = 1
# REPLICATED_SUBSPACE_CONFIG      = 2

basicResultDict = loopOptimizer(rho, N, CsByC, B, CuByC, Aavg, BASIC_SUBSPACE_CONFIG, triggerEnable, CtByC, Aq, CminByC)
repResultDict = loopOptimizer(rho, N, CsByC, B, CuByC, Aavg, REPLICATED_SUBSPACE_CONFIG, triggerEnable, CtByC, Aq, CminByC)

# compare which scheme is better, replicated or basic configuration
#functionValKey                  = 'funcValKey'
#optimalHKey                     = 'optimalHKey'

basicFuncValue = basicResultDict[functionValKey]
repFuncValue = repResultDict[functionValKey]

# basic config better
if( basicFuncValue <= repFuncValue ):
    print "BASIC OPTIMIZATION RESULT H "+str(basicResultDict[optimalHKey])+" MINVALUE "+str(basicFuncValue) \
    +" OTHERVAL "+str(repFuncValue)
else:
    print "REPLICATED OPTIMIZATION RESULT H "+str(repResultDict[optimalHKey])+" MINVALUE "+str(repFuncValue) \
    +" OTHERVAL "+str(basicFuncValue)
    

#print "number of nodes for trigger " +str(calculateOverlapingNodesForTrigger(18, 10))
#print "number nodes an update goes to "+str(calcluateExpectedNumNodesAnUpdateGoesTo(18, 10))