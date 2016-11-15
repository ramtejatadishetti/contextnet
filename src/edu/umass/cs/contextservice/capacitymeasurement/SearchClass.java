package edu.umass.cs.contextservice.capacitymeasurement;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

/**
 * Updates locations of all users after every 
 * granularityOfGeolocationUpdate
 * @author adipc
 */
public class SearchClass extends AbstractRequestSendingClass
{
	private final Random queryRand;
	private double sumResultSize = 0.0;
	private double sumTime = 0.0;
	private final double requestsps;
	
	private final double predicateLength;
	
	public SearchClass( int nodeId, int poolSize, int numAttrs, 
			double requestsps, double predicateLength )
	{
		super(nodeId, poolSize, 
				MySQLCapacityMeasurement.SEARCH_LOSS_TOLERANCE, numAttrs);
		
		queryRand = new Random();
		this.requestsps = requestsps;
		this.predicateLength = predicateLength;
	}
	
	
	@Override
	public void run()
	{
		try
		{
			this.startExpTime();
			searchQueryRateControlledRequestSender();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
	public double getAvgResultSize()
	{
		return sumResultSize/numRecvd;
	}
	
	public double getAvgTime()
	{
		return this.sumTime/numRecvd;
	}
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
	}
	
	@Override
	public void incrementSearchNumRecvd(int resultSize, long timeTaken)
	{
		synchronized(waitLock)
		{
			numRecvd++;
			
			sumResultSize = sumResultSize + resultSize;
			sumTime = sumTime + timeTaken;
			if( checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{
				waitLock.notify();
			}
		}
	}
	
	@Override
	public double startSendingRequests(double requestSendingRate) 
	{
		return 0;
	}
	
	private void searchQueryRateControlledRequestSender() throws Exception
	{
		// as it is per ms
		double reqspms = requestsps/1000.0;
		
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		while( ( (System.currentTimeMillis() - expStartTime)
				< MySQLCapacityMeasurement.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
//				sendQueryMessage(selectTableSQL);
				sendQueryMessageWithSmallRanges();
				//sendQueryMessage();
				numSent++;
			}
			currTime = System.currentTimeMillis();
			
			double timeElapsed = ((currTime- expStartTime)*1.0);
			double numberShouldBeSentByNow = timeElapsed*reqspms;
			double needsToBeSentBeforeSleep = numberShouldBeSentByNow - numSent;
			if(needsToBeSentBeforeSleep > 0)
			{
				needsToBeSentBeforeSleep = Math.ceil(needsToBeSentBeforeSleep);
			}
			
			for(int i=0;i<needsToBeSentBeforeSleep;i++)
			{
//				sendQueryMessage(selectTableSQL);
				sendQueryMessageWithSmallRanges();
				//sendQueryMessage();
				numSent++;
			}
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("Search eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Search result:Goodput "+sysThrput);
	}
	
	
	private void sendQueryMessage()
	{
		String searchQuery
			= "SELECT nodeGUID FROM "+MySQLCapacityMeasurement.DATE_TABLE_NAME+" WHERE ";
		
		HashMap<String, Boolean> distinctAttrMap 
			= pickDistinctAttrs( MySQLCapacityMeasurement.NUM_ATTRS_IN_QUERY, 
					numAttrs, queryRand );
		
		Iterator<String> attrIter = distinctAttrMap.keySet().iterator();
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			
			double attrMin 
				= 1 +queryRand.nextDouble()*
				(MySQLCapacityMeasurement.ATTR_MAX - MySQLCapacityMeasurement.ATTR_MIN);
		
			double predLength 
				= (queryRand.nextDouble()*
				(MySQLCapacityMeasurement.ATTR_MAX - MySQLCapacityMeasurement.ATTR_MIN));
			
			double attrMax = attrMin + predLength;
			// making it curcular
			
			// last so no AND
			if( !attrIter.hasNext() )
			{
				searchQuery = searchQuery + " "+attrName+" >= "+attrMin+" AND "+attrName
						+" <= "+attrMax;
			}
			else
			{
				searchQuery = searchQuery + " "+attrName+" >= "+attrMin+" AND "+attrName
					+" <= "+attrMax+" AND ";
			}
		}
		
		SearchTask searchTask = new SearchTask( searchQuery, this, dsInst );
		taskES.execute(searchTask);
	}
	
	
	private void sendQueryMessageWithSmallRanges()
	{
		String searchQuery
			= "SELECT nodeGUID FROM "+MySQLCapacityMeasurement.DATE_TABLE_NAME+" WHERE ( ";
		
		HashMap<String, Boolean> distinctAttrMap 
			= pickDistinctAttrs( MySQLCapacityMeasurement.NUM_ATTRS_IN_QUERY, 
					numAttrs, queryRand );
		
		Iterator<String> attrIter = distinctAttrMap.keySet().iterator();
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			
			double attrMin 
				= MySQLCapacityMeasurement.ATTR_MIN
				+queryRand.nextDouble()*(MySQLCapacityMeasurement.ATTR_MAX - 
						MySQLCapacityMeasurement.ATTR_MIN);
			
			// querying 10 % of domain
			double predLength 
				= ( predicateLength*(MySQLCapacityMeasurement.ATTR_MAX 
							- MySQLCapacityMeasurement.ATTR_MIN) );
		
			double attrMax = attrMin + predLength;
			
			// making it curcular
			if( attrMax > MySQLCapacityMeasurement.ATTR_MAX )
			{
				double diff = attrMax - MySQLCapacityMeasurement.ATTR_MAX;
				attrMax = MySQLCapacityMeasurement.ATTR_MIN + diff;
			}
		
			if( attrMin <= attrMax )
			{	
				String queryMin  = attrMin+"";
				String queryMax  = attrMax+"";
				
				if( !attrIter.hasNext() )
				{
					// it is assumed that the strings in query(pqc.getLowerBound() or pqc.getUpperBound()) 
					// will have single or double quotes in them so we don't need to them separately in mysql query
					searchQuery = searchQuery + " ( "+attrName +" >= "+queryMin +" AND " 
							+attrName +" <= "+queryMax+" ) )";
				}
				else
				{
					searchQuery = searchQuery + " ( "+attrName +" >= "+queryMin +" AND " 
							+attrName +" <= "+queryMax+" ) AND ";
				}
			}
			else
			{
				if( !attrIter.hasNext() )
				{
					String queryMin  = MySQLCapacityMeasurement.ATTR_MIN+"";
					String queryMax  = attrMax+"";
					
					searchQuery = searchQuery + " ( "
							+" ( "+attrName +" >= "+queryMin +" AND " 
							+attrName +" <= "+queryMax+" ) OR ";
					
					queryMin  = attrMin+"";
					queryMax  = MySQLCapacityMeasurement.ATTR_MAX+"";
					
					searchQuery = searchQuery +" ( "+attrName +" >= "+queryMin +" AND " 
							+attrName +" <= "+queryMax+" ) ) )";
				}
				else
				{
					String queryMin  = MySQLCapacityMeasurement.ATTR_MIN+"";
					String queryMax  = attrMax+"";
					
					searchQuery = searchQuery + " ( "
							+" ( "+attrName +" >= "+queryMin +" AND " 
							+attrName +" <= "+queryMax+" ) OR ";
							
					queryMin  = attrMin+"";
					queryMax  = MySQLCapacityMeasurement.ATTR_MAX+"";
					
					searchQuery = searchQuery +" ( "+attrName +" >= "+queryMin +" AND " 
							+attrName +" <= "+queryMax+" ) ) AND ";
				}
			}
		}	
		SearchTask searchTask = new SearchTask( searchQuery, this, dsInst );
		taskES.execute(searchTask);
	}
	
	
	private HashMap<String, Boolean> pickDistinctAttrs( int numAttrsToPick, 
			int totalAttrs, Random randGen )
	{
		HashMap<String, Boolean> hashMap = new HashMap<String, Boolean>();
		int currAttrNum = 0;
		while(hashMap.size() != numAttrsToPick)
		{
			if( numAttrs == 
					MySQLCapacityMeasurement.NUM_ATTRS_IN_QUERY)
			{
				String attrName = "attr"+currAttrNum;
				hashMap.put(attrName, true);				
				currAttrNum++;
			}
			else
			{
				currAttrNum = randGen.nextInt(numAttrs);
				String attrName = "attr"+currAttrNum;
				hashMap.put(attrName, true);
			}
		}
		return hashMap;
	}
}