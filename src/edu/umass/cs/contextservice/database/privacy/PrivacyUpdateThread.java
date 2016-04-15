package edu.umass.cs.contextservice.database.privacy;

import java.util.HashMap;

import edu.umass.cs.contextservice.messages.dataformat.AttrValueRepresentationJSON;

/**
 * This class implements the insert  of privacy information.
 * It implements runnable, so that it can be done in parallel
 * with update to guidAttrValue storage tables.
 * @author adipc
 */
public class PrivacyUpdateThread implements Runnable
{
	public static final int PERFORM_INSERT				= 1;
	public static final int PERFORM_DELETION			= 2;
	
	private final int operation;
	private final String ID;
	private final HashMap<String, AttrValueRepresentationJSON> atrToValueRep;
	private final int subspaceId;
	private final PrivacyInformationStorageInterface privacyInformationStorage;
	
	public PrivacyUpdateThread( String ID, 
    		HashMap<String, AttrValueRepresentationJSON> atrToValueRep, int subspaceId, 
    		PrivacyInformationStorageInterface privacyInformationStorage )
	{
		this.operation = PERFORM_INSERT;
		this.ID = ID;
		this.atrToValueRep = atrToValueRep;
		this.subspaceId = subspaceId;
		this.privacyInformationStorage = privacyInformationStorage;
	}
	
	public PrivacyUpdateThread( String ID, int subspaceId, 
    		PrivacyInformationStorageInterface privacyInformationStorage )
	{
		this.operation = PERFORM_DELETION;
		this.ID = ID;
		this.subspaceId = subspaceId;
		atrToValueRep  = null;
		this.privacyInformationStorage = privacyInformationStorage;
	}
	
	
	@Override
	public void run()
	{
		if( operation == PERFORM_INSERT )
		{
			privacyInformationStorage.bulkInsertPrivacyInformation
			(ID, atrToValueRep, subspaceId);
		}
		else if( operation == PERFORM_DELETION )
		{
			privacyInformationStorage.deleteAnonymizedIDFromPrivacyInfoStorage
			(ID, subspaceId);
		}
	}
}