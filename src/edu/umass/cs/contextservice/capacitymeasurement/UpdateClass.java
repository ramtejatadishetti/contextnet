package edu.umass.cs.contextservice.capacitymeasurement;


import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Updates locations of all users after every 
 * granularityOfGeolocationUpdate
 * @author adipc
 */
public class UpdateClass extends AbstractRequestSendingClass
{
	private Random updateRand;
	
	private double sumUpdTime = 0;
	
	private final double requestsps;
	private final int numGuids;
	
	public UpdateClass(int nodeId, int poolSize, double lossTolerance, 
			int numAttrs, double requestsps, int numGuids)
	{
		super(nodeId, poolSize, MySQLCapacityMeasurement.UPD_LOSS_TOLERANCE, numAttrs);
		updateRand = new Random();
		this.requestsps = requestsps;
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
		double reqspms = requestsps/1000.0;
		long currTime  = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double currUserGuidNum   = 0;
		
		while( ( (System.currentTimeMillis() - expStartTime) 
						< MySQLCapacityMeasurement.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				doUpdate((int)currUserGuidNum);
				currUserGuidNum++;
				currUserGuidNum=((int)currUserGuidNum)%numGuids;
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
				doUpdate((int)currUserGuidNum);
				currUserGuidNum++;
				currUserGuidNum=((int)currUserGuidNum)%numGuids;
			}
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("Update eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Update result:Goodput "+sysThrput);
	}
	
	
	private void doUpdate(int currUserGuidNum) throws JSONException
	{
		numSent++;
		String guid = MySQLCapacityMeasurement.getSHA1
				(MySQLCapacityMeasurement.GUID_PREFIX+currUserGuidNum);
		
		String attrName = "attr"+updateRand.nextInt(numAttrs);
		double value = 1500*+updateRand.nextDouble();
		JSONObject updateJSON = new JSONObject();
		
		updateJSON.put(attrName, value);
		
		UpdateTask updTask = new UpdateTask( guid, updateJSON, this, dsInst);
		taskES.execute(updTask);
	}
	
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			sumUpdTime = sumUpdTime+timeTaken;
			
			if(checkForCompletionWithLossTolerance(numSent, numRecvd))
			{
				waitLock.notify();
			}
		}
	}
	
	public double getAvgUpdateTime()
	{
		return sumUpdTime/numRecvd;
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