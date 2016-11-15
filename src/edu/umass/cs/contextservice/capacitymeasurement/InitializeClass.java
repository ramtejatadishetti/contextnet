package edu.umass.cs.contextservice.capacitymeasurement;

import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

public class InitializeClass extends AbstractRequestSendingClass
{
	private Random updateRand;
	private final int numGuids;
	
	public InitializeClass(int nodeId, int poolSize, double lossTolerance, 
			int numAttrs, int numGuids )
	{
		super(nodeId, poolSize, MySQLCapacityMeasurement.INSERT_LOSS_TOLERANCE, numAttrs);
		updateRand = new Random();
		this.numGuids = numGuids;
	}
	
	@Override
	public void run()
	{
		try 
		{
			this.startExpTime();
			updRateControlledRequestSender();
		} catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
	
	private void updRateControlledRequestSender() throws Exception
	{
		double reqspms = 100.0/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double currUserGuidNum   = 0;
		
		while( ( currUserGuidNum < numGuids ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				doUpdate((int)currUserGuidNum);
				currUserGuidNum++;
				if( currUserGuidNum >= numGuids )
					break;
			}
			if( currUserGuidNum >= numGuids )
				break;
			
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
				doUpdate((int)currUserGuidNum);
				currUserGuidNum++;
				if( currUserGuidNum >= numGuids )
					break;
			}
			if( currUserGuidNum >= numGuids )
				break;
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("Init eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Init result:Goodput "+sysThrput);
	}
	
	
	private void doUpdate(int currUserGuidNum)
	{
		numSent++;
		String guid = MySQLCapacityMeasurement.getSHA1
							(MySQLCapacityMeasurement.GUID_PREFIX+currUserGuidNum);
		
		JSONObject attrValJSON = new JSONObject();
		
		for(int i=0; i<numAttrs; i++)
		{
			try 
			{
				attrValJSON.put("attr"+i, 1500 * updateRand.nextDouble());
			}
			catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
		
		InitializeTask updTask = new InitializeTask( guid, attrValJSON, this, dsInst);
		
		taskES.execute(updTask);
	}

	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			System.out.println("Init reply recvd "+userGUID+" time taken "+timeTaken+
					" numSent "+numSent+" numRecvd "+numRecvd);
			//if(currNumReplyRecvd == currNumReqSent)
			if(checkForCompletionWithLossTolerance(numSent, numRecvd))
			{
				waitLock.notify();
			}
		}
	}
	
	@Override
	public void incrementSearchNumRecvd(int resultSize, long timeTaken) 
	{
	}

	@Override
	public double startSendingRequests(double requestSendingRate) 
	{
		return 0;
	}
}