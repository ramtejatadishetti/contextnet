package edu.umass.cs.contextservice.gns;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.logging.Logger;

import org.json.JSONArray;

import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.utils.Utils;

/**
 * used to bypass gns as it doesn't work most of the time.
 * @author adipc
 *
 */
public class GNSCallsNoCalls
{
		//public static String gnsHost = "ananas.cs.umass.edu";
		//NIO LNS port
		//public static int gnsPort = 24398;
		
//		public static enum UserGUIDOperations {ADD_USER_GUID_TO_GROUP, REMOVE_USER_GUID_FROM_GROUP};
//		private static final String defaultGns = KeyPairUtils.getDefaultGns();
//		private static final UniversalTcpClient gnsClient 
//					= new UniversalTcpClient(defaultGns.split(":")[0], Integer.parseInt(defaultGns.split(":")[1]));
//		
//		private static final GuidEntry myGuidEntry = KeyPairUtils.getDefaultGuidEntry(defaultGns);
//		
//		private final static Logger log = ContextServiceLogger.getLogger();
//			
		/**
		 * takes alias of group, which is query and reads groupmembers
		 * @param query
		 * @return guids of group members
		 */
		public static JSONArray readGroupMembers(String query)
		{
			return new JSONArray();
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
			String queryHash = Utils.getSHA1(queryString);
			String groupGUIDString = queryHash.substring(0, 20);
			//log.fine("createQueryGroup groupGUID returned "+groupGUIDString);
			return groupGUIDString;
		}
		
		/**
		 * Adds multiple User GUIDs to a single group
		 * @param guidsList
		 * @param queryString
		 */
		public static void addGUIDsToGroup(JSONArray guidsList, String queryString)
		{
		}
		
		/**
		 * removes a single user GUID from list of group GUIDs
		 * @param guidsList
		 * @param queryString
		 */
		/*public static void userGUIDAndGroupGUIDOperations(String userGUID, LinkedList<GroupGUIDRecord> groupGUIDList,
				UserGUIDOperations oper)
		{
		}*/
			
			
		/**
		 * adds the given address in the notification set
		 * @param socketAddress
		 * @param groupQuery
		 */
		public static void updateNotificationSetOfAGroup(InetSocketAddress socketAddress, String groupQuery)
		{
		}	
			
		public static JSONArray getNotificationSetOfAGroup(String groupQuery)
		{
			return new JSONArray();
		}
		
		public static void clearNotificationSetOfAGroup(String groupQuery)
		{
		}
}