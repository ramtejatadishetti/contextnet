package edu.umass.cs.contextservice.client.profiler;



public class ClientProfilerStatClass implements Runnable
{
	private long numSearchSent 						= 0;
	private long numSearchRepRecvd 					= 0;
	
	private long numUpdateSent 						= 0;
	private long numUpdateRepRecvd					= 0;
	
	
	private final Object lock 						= new Object();
	
	@Override
	public void run()
	{
		while(true)
		{
			try
			{
				Thread.sleep(10000);
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			}
			
			System.out.println("numUpdateSent "+numUpdateSent
								+" numUpdateRepRecvd "+numUpdateRepRecvd
								+" numSearchSent "+numSearchSent
								+" numSearchRepRecvd "+numSearchRepRecvd);
		}
	}
	
	public void incrementNumSearchSent()
	{
		synchronized(lock)
		{
			numSearchSent++;
		}
	}
	
	public void incrementNumSearchRepRecvd()
	{
		synchronized(lock)
		{
			numSearchRepRecvd++;
		}
	}
	
	public void incrementNumUpdateSent()
	{
		synchronized( lock )
		{
			numUpdateSent++;
		}
	}
	
	public void incrementUpdateRepRecvd()
	{
		synchronized( lock )
		{
			numUpdateRepRecvd++;
		}
	}
}