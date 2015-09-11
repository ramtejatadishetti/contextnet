package edu.umass.cs.contextservice.gns;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.util.LinkedList;
import java.util.logging.Logger;

import org.json.JSONArray;

import edu.umass.cs.contextservice.database.records.GroupGUIDRecord;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gns.client.GnsProtocol;
import edu.umass.cs.gns.client.GuidEntry;
import edu.umass.cs.gns.client.UniversalTcpClient;
import edu.umass.cs.gns.client.util.KeyPairUtils;
import edu.umass.cs.gns.exceptions.GnsException;

/**
 * GNS Calls required for context service. The group calls
 * supported don't require keys for update. They have all write permissions
 * @author adipc
 *
 */
public class GNSCalls
{
		//public static String gnsHost = "ananas.cs.umass.edu";
		//NIO LNS port
		//public static int gnsPort = 24398;
		
		public static enum UserGUIDOperations {ADD_USER_GUID_TO_GROUP, REMOVE_USER_GUID_FROM_GROUP};
		private static final String defaultGns = KeyPairUtils.getDefaultGnsFromPreferences();
		private static final UniversalTcpClient gnsClient 
					= new UniversalTcpClient(defaultGns.split(":")[0], Integer.parseInt(defaultGns.split(":")[1]));
		
		private static final GuidEntry myGuidEntry = KeyPairUtils.getDefaultGuidEntryFromPreferences(defaultGns);
		
		private final static Logger log = ContextServiceLogger.getLogger();
		
		/**
		 * takes alias of group, which is query and reads groupmembers
		 * @param query
		 * @return guids of group members
		 */
		public static JSONArray readGroupMembers(String query)
		{
			/*String defaultGns = KeyPairUtils.getDefaultGnsFromPreferences();
			String[] parsed = defaultGns.split(":");
			String gnsHost = parsed[0];
			int gnsPort = Integer.parseInt(parsed[1]);
			UniversalTcpClient gnsClient = new UniversalTcpClient(gnsHost, gnsPort);*/
			
			JSONArray grpMem = null;
			
			try
			{
				String queryHash = Utils.getSHA1(query);	
				//GuidEntry myGuidEntry = KeyPairUtils.getDefaultGuidEntryFromPreferences(defaultGns);
				
				String guidString = gnsClient.lookupGuid(queryHash);
				// group should be read by all, atleast for now
				grpMem = gnsClient.groupGetMembers(guidString, myGuidEntry);
			} catch (UnsupportedEncodingException e)
			{
				e.printStackTrace();
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (GnsException e)
			{
				log.info("GnsException no group exists");
				//e.printStackTrace();
			}
			return grpMem;
		}
		
		/**
		 * creates the group for a query, adding 
		 * query as the alias.
		 * Initially the group is empty but 
		 * GUIDs are added as soon as query processing is 
		 * complete.
		 * @return groupGUID if successful otherwise empty string
		 */
		public static String createQueryGroup(String queryString)
		{	
			String groupGUIDString = "";
			try
			{
				String queryHash = Utils.getSHA1(queryString);
				
				/*String defaultGns = KeyPairUtils.getDefaultGnsFromPreferences();
				System.out.println(" createQueryGroup defaultGns "+defaultGns);
				
				String[] parsed = defaultGns.split(":");
				String gnsHost = parsed[0];
				int gnsPort = Integer.parseInt(parsed[1]);
				
				GuidEntry myGuidEntry = KeyPairUtils.getDefaultGuidEntryFromPreferences(defaultGns);
				UniversalTcpClient gnsClient = new UniversalTcpClient(gnsHost, gnsPort);*/
			   //GuidEntry groupGuid = KeyPairUtils.getGuidEntryFromPreferences(defaultGns, queryHash);
			    
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
			    		groupGUIDString = gnsClient.lookupGuid(queryHash);
			    		groupFound = true;
			    	} catch(Exception ex)
			    	{
			    		// lookup failed, create the group.
			    		groupFound = false;
			    	}
			    	
			    	//create the group
			    	if(!groupFound)
			    	{
			    		log.info("No group exisits " + queryString + " and hash "+queryHash+". Generating new GUID and keys");
			    		// Create a new GUID
			    		GuidEntry groupGuid = gnsClient.guidCreate(myGuidEntry, queryHash);
			    		
			    		// save keys in the preference
			    		System.out.println("saving keys to local");
			    		KeyPairUtils.saveKeyPairToPreferences(KeyPairUtils.getDefaultGnsFromPreferences(), 
			    			  groupGuid.getEntityName() , groupGuid.getGuid(), 
			    			  new KeyPair(groupGuid.getPublicKey(), groupGuid.getPrivateKey()));
			
			    		// storing alias in gns record, need it to find it when we have GUID
			    		// from group members
			    		
			    		//gnsClient.addAlias(groupGuid, queryHash);
			    		groupGUIDString = groupGuid.getGuid();
			    		
			    		// a group should be readable and writable by all, so that we don't need 
			    		// to move keys
			    		gnsClient.groupAddMembershipReadPermission(groupGuid, GnsProtocol.ALL_USERS);
			    		gnsClient.groupAddMembershipUpdatePermission(groupGuid, GnsProtocol.ALL_USERS);
			    		//gnsClient.fieldCreate(groupGuid.getGuid(), ALIAS_FIELD, new JSONArray().put(name), myGuid);
			    	} else // reset the group
			    	{
			    		log.info("group already exists, just reseting the group "+groupGUIDString);
			    		//FIXME: need one command to reset the group.
			    		JSONArray jsonMem = gnsClient.groupGetMembers(groupGUIDString, myGuidEntry);
			    		if(jsonMem.length() > 0)
			    		{
			    			log.info("number of members already in group "+jsonMem.length());
			    			gnsClient.groupRemoveGuids(groupGUIDString, jsonMem, myGuidEntry);
			    		}
			    	}
			    	
			    	//Put the IP address in the GNS
			    	//String ipPort = saddr.getAddress().getHostAddress() + ":" + saddr.getPort();
			    	//log.trace("Updating " + GnsConstants.SERVER_REG_ADDR + " GNSValue " + ipPort);
			    	//gnsClient
			    	//.fieldReplaceOrCreate(myGuid.getGuid(), GnsConstants.SERVER_REG_ADDR, 
			    	//new JSONArray().put(ipPort), myGuid);
			    }
			} catch(Exception ex)
			{
				ex.printStackTrace();
			}
			System.out.println("createQueryGroup groupGUID returned "+groupGUIDString);
			return groupGUIDString;
		}
		
		/**
		 * Adds multiple User GUIDs to a single group
		 * @param guidsList
		 * @param queryString
		 */
		public static void addGUIDsToGroup(JSONArray guidsList, String queryString)
		{
			try
			{
				String queryHash = Utils.getSHA1(queryString);
				
				/*String defaultGns = KeyPairUtils.getDefaultGnsFromPreferences();
				String[] parsed = defaultGns.split(":");
				String gnsHost = parsed[0];
				int gnsPort = Integer.parseInt(parsed[1]);
				GuidEntry myGuidEntry = KeyPairUtils.getDefaultGuidEntryFromPreferences(defaultGns);
				UniversalTcpClient gnsClient = new UniversalTcpClient(gnsHost, gnsPort);*/
				
			    // GuidEntry groupGuid = KeyPairUtils.getGuidEntryFromPreferences(defaultGns, queryHash);
			    
			    /*
			     * Take a lock on the GNS connection object to prevent concurrent queries to
			     * the GNS on the same connection as the library is not thread-safe from
			     * that standpoint.
			     */
			    synchronized (gnsClient)
			    {
			      //if(groupGuid!=null)
			      {
			    	  String groupGUIDString = gnsClient.lookupGuid(queryHash);
			    	  gnsClient.groupAddGuids(groupGUIDString, guidsList, myGuidEntry);
			      }
			      /*else
			      {
			    	  assert(false);
			      }*/
			    }
			} catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
		
		
		/**
		 * removes a single user GUID from list of group GUIDs
		 * @param guidsList
		 * @param queryString
		 */
		public static void userGUIDAndGroupGUIDOperations(String userGUID, LinkedList<GroupGUIDRecord> groupGUIDList,
				UserGUIDOperations oper)
		{
			try
			{
				/*String defaultGns = KeyPairUtils.getDefaultGnsFromPreferences();
				
				String[] parsed = defaultGns.split(":");
				String gnsHost = parsed[0];
				int gnsPort = Integer.parseInt(parsed[1]);
				GuidEntry myGuidEntry = KeyPairUtils.getDefaultGuidEntryFromPreferences(defaultGns);
				UniversalTcpClient gnsClient = new UniversalTcpClient(gnsHost, gnsPort);*/
				
				for(int i=0;i<groupGUIDList.size();i++)
				{
					GroupGUIDRecord grpGUIDInfo = groupGUIDList.get(i);
					String queryString = grpGUIDInfo.getGroupQuery();
					
					String queryHash = Utils.getSHA1(queryString);
					
					//GuidEntry groupGuid = KeyPairUtils.getGuidEntryFromPreferences(defaultGns, queryHash);
				    
				    /*
				     * Take a lock on the GNS connection object to prevent concurrent queries to
				     * the GNS on the same connection as the library is not thread-safe from
				     * that standpoint.
				     */
				    synchronized (gnsClient)
				    {
				      String groupGUIDString = gnsClient.lookupGuid(queryHash);
				      //if(groupGuid!=null)
				      //{
				    	  switch(oper)
				    	  {
				    	  	case ADD_USER_GUID_TO_GROUP:
				    	  	{
				    	  		System.out.println("ADD_USER_GUID_TO_GROUP "+" groupGUIDString "+
				    	  				groupGUIDString+" userGUID "+userGUID);
				    	  		
				    	  		gnsClient.groupAddGuid(groupGUIDString, userGUID, myGuidEntry);
				    	  		break;
				    	  	}
				    	  	case REMOVE_USER_GUID_FROM_GROUP:
				    	  	{
				    	  		System.out.println("REMOVE_USER_GUID_FROM_GROUP "+" groupGUIDString "+
				    	  				groupGUIDString+" userGUID "+userGUID);
				    	  		gnsClient.groupRemoveGuid(groupGUIDString, userGUID, myGuidEntry);
				    	  		break;
				    	  	}
				    	  }
				      //}
				      /*else
				      {
				    	  assert(false);
				      }*/
				    }
				}
			} catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
		
		
		/**
		 * adds the given address in the notification set
		 * @param socketAddress
		 * @param groupQuery
		 */
		public static void updateNotificationSetOfAGroup(InetSocketAddress socketAddress, String groupQuery)
		{
			try
			{
				/*String defaultGns = KeyPairUtils.getDefaultGnsFromPreferences();
				
				String[] parsed = defaultGns.split(":");
				String gnsHost = parsed[0];
				int gnsPort = Integer.parseInt(parsed[1]);
				GuidEntry myGuidEntry = KeyPairUtils.getDefaultGuidEntryFromPreferences(defaultGns);
				UniversalTcpClient gnsClient = new UniversalTcpClient(gnsHost, gnsPort);*/
				
				String queryHash = Utils.getSHA1(groupQuery);
				
				//GuidEntry groupGuid = KeyPairUtils.getGuidEntryFromPreferences(defaultGns, queryHash);
				  
			    /*
			     * Take a lock on the GNS connection object to prevent concurrent queries to
			     * the GNS on the same connection as the library is not thread-safe from
			     * that standpoint.
			     */
			    synchronized (gnsClient)
			    {
			      //if(groupGuid!=null)
			      {
			    	  String groupGUIDString = gnsClient.lookupGuid(queryHash);
			    	  String addrString = socketAddress.getAddress().getHostAddress()+":"+socketAddress.getPort();
			    	  JSONArray arr = new JSONArray();
			    	  arr.put(addrString);
			    	  gnsClient.fieldAppend(groupGUIDString, GnsConstants.NOTIFICATION_SET, arr, myGuidEntry);
			      }
			      /*else
			      {
			    	  assert(false);
			      }*/
			    }
			} catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
		
		
		public static JSONArray getNotificationSetOfAGroup(String groupQuery)
		{
			JSONArray result = new JSONArray();
			try
			{
				/*String defaultGns = KeyPairUtils.getDefaultGnsFromPreferences();
				
				String[] parsed = defaultGns.split(":");
				String gnsHost = parsed[0];
				int gnsPort = Integer.parseInt(parsed[1]);
				GuidEntry myGuidEntry = KeyPairUtils.getDefaultGuidEntryFromPreferences(defaultGns);
				UniversalTcpClient gnsClient = new UniversalTcpClient(gnsHost, gnsPort);*/
				
				String queryHash = Utils.getSHA1(groupQuery);
				
				//GuidEntry groupGuid = KeyPairUtils.getGuidEntryFromPreferences(defaultGns, queryHash);
				
			    /*
			     * Take a lock on the GNS connection object to prevent concurrent queries to
			     * the GNS on the same connection as the library is not thread-safe from
			     * that standpoint.
			     */
			    synchronized (gnsClient)
			    {
			      //if(groupGuid!=null)
			      {
			    	  String groupGUIDString = gnsClient.lookupGuid(queryHash);
			    	  result = gnsClient.fieldReadArray(groupGUIDString, GnsConstants.NOTIFICATION_SET, myGuidEntry);
			      }
			      /*else
			      {
			    	  assert(false);
			      }*/
			    }
			} catch(Exception ex)
			{
				ex.printStackTrace();
			}
			return result;
		}
		
		
		public static void clearNotificationSetOfAGroup(String groupQuery)
		{
			try
			{
				/*String defaultGns = KeyPairUtils.getDefaultGnsFromPreferences();
				
				String[] parsed = defaultGns.split(":");
				String gnsHost = parsed[0];
				int gnsPort = Integer.parseInt(parsed[1]);
				GuidEntry myGuidEntry = KeyPairUtils.getDefaultGuidEntryFromPreferences(defaultGns);
				UniversalTcpClient gnsClient = new UniversalTcpClient(gnsHost, gnsPort);*/
				
				String queryHash = Utils.getSHA1(groupQuery);
				
				//GuidEntry groupGuid = KeyPairUtils.getGuidEntryFromPreferences(defaultGns, queryHash);
				    
			    /*
			     * Take a lock on the GNS connection object to prevent concurrent queries to
			     * the GNS on the same connection as the library is not thread-safe from
			     * that standpoint.
			     */
			    synchronized (gnsClient)
			    {
			      //if(groupGuid!=null)
			      {
			    	  String groupGUIDString = gnsClient.lookupGuid(queryHash);
			    	  gnsClient.fieldClear(groupGUIDString, GnsConstants.NOTIFICATION_SET, myGuidEntry);
			      }
			      /*else
			      {
			    	  assert(false);
			      }*/
			    }
			} catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
		
		
		/*public static void checkIfAttributeMetadataExists(String attributeName)
		{
			try
			 {
			    UniversalGnsClient gnsClient = new UniversalGnsClient(gnsHost, gnsPort);
			    String guidString = gnsClient.lookupGuid(attributeName);
			    	    
			    JSONArray resultArray;
			    // Read from the GNS
			    synchronized (gnsClient)
			    {
			      resultArray = gnsClient.fieldRead(guidString, GnsConstants.SERVER_REG_ADDR, null);
			    }
			    Vector<InetSocketAddress> resultVector = new Vector<InetSocketAddress>();
			    for (int i = 0; i < resultArray.length(); i++)
			    {
			      String str = resultArray.getString(i);
			      log.fine("Value returned from GNS " + str);
			      String[] Parsed = str.split(":");
			      InetSocketAddress socketAddress = new InetSocketAddress(Parsed[0], Integer.parseInt(Parsed[1]));
			      resultVector.add(socketAddress);
			    }
			    return resultVector;
			 } catch(Exception ex)
			 {
				 ex.printStackTrace();
				 throw new UnknownHostException(ex.toString());
			 }
		}*/
		
		/**
		 * Checks if the group for the given query already 
		 * exists. Query is used as an alias to look for the
		 * GroupGUID, if that fails then no such group exisits.
		 * Works only if queries are exactly same in string
		 * representation.
		 * @param query
		 * @return
		 */
		/*public static String checkIfGroupForQueryExists(String query)
		{
			String defaultGns = KeyPairUtils.getDefaultGnsFromPreferences();
			String[] parsed = defaultGns.split(":");
			String gnsHost = parsed[0];
			int gnsPort = Integer.parseInt(parsed[1]);
			
			String guidString = "";
			UniversalTcpClient gnsClient = new UniversalTcpClient(gnsHost, gnsPort);
			
			try
			{
				guidString = gnsClient.lookupGuid(query);
				
			} catch (UnsupportedEncodingException e)
			{
				e.printStackTrace();
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (GnsException e)
			{
				log.info("GnsException no group exists");
				//e.printStackTrace();
			}
			return guidString;
		}*/
}