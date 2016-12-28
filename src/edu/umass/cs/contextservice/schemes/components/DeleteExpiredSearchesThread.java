package edu.umass.cs.contextservice.schemes.components;


import edu.umass.cs.contextservice.database.AbstractDataStorageDB;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;

public class DeleteExpiredSearchesThread implements Runnable
{
	private final AbstractDataStorageDB dataStorageDB;
	
	public DeleteExpiredSearchesThread(AbstractDataStorageDB dataStorageDB)
	{
		this.dataStorageDB = dataStorageDB;
	}
	
	@Override
	public void run() 
	{
		while( true )
		{
			try
			{
				Thread.sleep(1000);
			} catch (InterruptedException e) 
			{
				e.printStackTrace();
			}
			
			int numDeleted = dataStorageDB.deleteExpiredSearchQueries();
					if(numDeleted > 0)
						ContextServiceLogger.getLogger().fine( "Group guids deleted "
							+" numDeleted "+numDeleted );
		}
	}
}