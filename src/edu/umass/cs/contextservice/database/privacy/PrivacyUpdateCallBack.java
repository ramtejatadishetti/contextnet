package edu.umass.cs.contextservice.database.privacy;

import java.util.HashMap;

import org.json.JSONObject;

import edu.umass.cs.contextservice.messages.dataformat.AttrValueRepresentationJSON;

/**
 * This class implements the insert  of privacy information.
 * It implements runnable, so that it can be done in parallel
 * with update to guidAttrValue storage tables.
 * @author adipc
 */
public class PrivacyUpdateCallBack implements Runnable
{
	public static final int PERFORM_INSERT				= 1;
	public static final int PERFORM_DELETION			= 2;
	
	private final int operation;
	private final String ID;
	private final HashMap<String, AttrValueRepresentationJSON> atrToValueRep;
	private final int subspaceId;
	private final JSONObject oldValJSON;
	private final PrivacyInformationStorageInterface privacyInformationStorage;
	private boolean finished = false;
	
	private final Object lock = new Object();
	
	public PrivacyUpdateCallBack( String ID, 
    		HashMap<String, AttrValueRepresentationJSON> atrToValueRep, int subspaceId, 
    		JSONObject oldValJSON, PrivacyInformationStorageInterface privacyInformationStorage )
	{
		this.operation = PERFORM_INSERT;
		this.ID = ID;
		this.atrToValueRep = atrToValueRep;
		this.subspaceId = subspaceId;
		this.oldValJSON = oldValJSON;
		this.privacyInformationStorage = privacyInformationStorage;
		finished = false;
	}
	
	public PrivacyUpdateCallBack( String ID, int subspaceId, 
    		PrivacyInformationStorageInterface privacyInformationStorage )
	{
		this.operation = PERFORM_DELETION;
		this.ID = ID;
		this.subspaceId = subspaceId;
		atrToValueRep  = null;
		oldValJSON = null;
		this.privacyInformationStorage = privacyInformationStorage;
		finished = false;
	}
	
	
	@Override
	public void run()
	{
		try
		{
			if( operation == PERFORM_INSERT )
			{
				privacyInformationStorage.bulkInsertPrivacyInformationBlocking
				(ID, atrToValueRep, subspaceId, oldValJSON);
			}
			else if( operation == PERFORM_DELETION )
			{
				privacyInformationStorage.deleteAnonymizedIDFromPrivacyInfoStorageBlocking
				(ID, subspaceId);
			}
			synchronized(lock)
			{
				finished = true;
				lock.notify();
			}
		}
		catch(Exception | Error ex)
		{
			ex.printStackTrace();
		}
	}
	
	public void waitForFinish()
	{
		synchronized(lock)
		{
			while( !finished )
			{
				try 
				{
					lock.wait();
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}
	
}