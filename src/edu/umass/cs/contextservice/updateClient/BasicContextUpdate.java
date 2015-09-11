package edu.umass.cs.contextservice.updateClient;

import java.io.IOException;
import java.security.KeyPair;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.gns.client.GuidEntry;
import edu.umass.cs.gns.client.UniversalTcpClient;
import edu.umass.cs.gns.client.util.KeyPairUtils;
import edu.umass.cs.gns.exceptions.GnsException;

public class BasicContextUpdate
{
	// per 1000msec
	public static int ATTR_UPDATE_RATE								= 5000;
	private static final String defaultGns 							= KeyPairUtils.getDefaultGnsFromPreferences();
	private static final UniversalTcpClient gnsClient 
				= new UniversalTcpClient(defaultGns.split(":")[0], Integer.parseInt(defaultGns.split(":")[1]));
	
	private static final GuidEntry myGuidEntry = KeyPairUtils.getDefaultGuidEntryFromPreferences(defaultGns);
	
	public static final int NUMGUIDs									= 1;
	// prefix for client GUIDs clientGUID1, client GUID2, ...
	public static final String CLIENT_GUID_PREFIX					= "clientGUID";
	
	public static final Random rand 									= new Random();
	
	
	/**
	 * creates GUID and adds all attributes
	 */
	public static String createGUID(int clientID)
	{
		String guidString = "";
		String guidAlias = CLIENT_GUID_PREFIX+clientID;
		
		try
		{   
		    /*
		     * Take a lock on the GNS connection object to prevent concurrent queries to
		     * the GNS on the same connection as the library is not thread-safe from
		     * that standpoint.
		     */
		    synchronized (gnsClient)
		    {
		    	boolean groupFound = false;
		    	try
		    	{
		    		// if lookup succeeds then there is a group that exists,
		    		// just reset it. If lookup doesn't succeed then 
		    		// create the group.
		    		guidString = gnsClient.lookupGuid(guidAlias);
		    		groupFound = true;
		    	} catch(Exception ex)
		    	{
		    		// lookup failed, create the group.
		    		groupFound = false;
		    	}
		    	
		    	//create the group
		    	if(!groupFound)
		    	{
		    		System.out.println
		    		("No group exisits " + guidAlias+". Generating new GUID and keys");
		    		// Create a new GUID
		    		GuidEntry guidEntry = gnsClient.guidCreate
		    				(myGuidEntry, guidAlias);
		    		
		    		// save keys in the preference
		    		System.out.println("saving keys to local");
		    		KeyPairUtils.saveKeyPairToPreferences(KeyPairUtils.getDefaultGnsFromPreferences(), 
		    				guidEntry.getEntityName() , guidEntry.getGuid(), 
		    			  new KeyPair(guidEntry.getPublicKey(), guidEntry.getPrivateKey()));
		    		
		    		// storing alias in gns record, need it to find it when we have GUID
		    		// from group members
		    		
		    		//gnsClient.addAlias(groupGuid, queryHash);
		    		guidString = guidEntry.getGuid();
		    		
		    		// a group should be readable and writable by all, so that we don't need 
		    		// to move keys
		    		//gnsClient.groupAddMembershipReadPermission(groupGuid, GnsProtocol.ALL_USERS);
		    		//gnsClient.groupAddMembershipUpdatePermission(groupGuid, GnsProtocol.ALL_USERS);
		    		//gnsClient.fieldCreate(groupGuid.getGuid(), ALIAS_FIELD, new JSONArray().put(name), myGuid);
		    	} else // reset the group
		    	{
		    		
		    	}
		    	
		    	for(int i=0;i<ContextServiceConfig.NUM_ATTRIBUTES;i++)
		    	{
		    		GuidEntry guidEntry = KeyPairUtils.getGuidEntryFromPreferences
		    				(KeyPairUtils.getDefaultGnsFromPreferences(), guidAlias);
		    		
		    		gnsClient.fieldCreateList
		    		(guidString, ContextServiceConfig.CONTEXT_ATTR_PREFIX+i, new JSONArray().put("100"), guidEntry);
		    	}
		    }
		} catch(Exception ex)
		{
			ex.printStackTrace();
		}
		return guidString;
	}
	
	/**
	 * does the attribute updates at a rate.
	 */
	public static void doAttributeUpdates(String guidString, String guidAlias)
	{
		GuidEntry guidEntry = KeyPairUtils.getGuidEntryFromPreferences
				(KeyPairUtils.getDefaultGnsFromPreferences(), guidAlias);
		
		for(int i=0;i<ContextServiceConfig.NUM_ATTRIBUTES;i++)
		{
			String attName = ContextServiceConfig.CONTEXT_ATTR_PREFIX+i;
			double nextVal = 1+rand.nextInt((int)(AttributeTypes.MAX_VALUE-AttributeTypes.MIN_VALUE));
			try
			{
				gnsClient.fieldUpdateAsynch(guidString, attName, nextVal, guidEntry);
			} catch (GnsException e) 
			{
				e.printStackTrace();
			} catch (IOException e) 
			{
				e.printStackTrace();
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args)
	{
		int clientID = Integer.parseInt(args[0]);
		
		ATTR_UPDATE_RATE = Integer.parseInt(args[1]);
		
		String guidAlias = CLIENT_GUID_PREFIX+clientID;
		String guidString = createGUID(clientID);
		
		while(true)
		{
			try 
			{
				doAttributeUpdates(guidString, guidAlias);
				Thread.sleep(ATTR_UPDATE_RATE);
			} catch (InterruptedException e) 
			{
				e.printStackTrace();
			}
		}
	}
	
}