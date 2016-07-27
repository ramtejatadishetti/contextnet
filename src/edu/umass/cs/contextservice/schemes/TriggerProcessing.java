package edu.umass.cs.contextservice.schemes;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.HyperspaceMySQLDB;
import edu.umass.cs.contextservice.database.triggers.GroupGUIDInfoClass;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegion;
import edu.umass.cs.contextservice.messages.RefreshTrigger;
import edu.umass.cs.contextservice.messages.ValueUpdateToSubspaceRegionMessage;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.nio.JSONMessenger;

/**
 * Implements trigger processing interface.
 * Implements hyperspace trigger processing interface.
 * @author adipc
 *
 */
public class TriggerProcessing<NodeIDType> implements 
								TriggerProcessingInterface<NodeIDType>
{
	private final HyperspaceMySQLDB<NodeIDType> hyperspaceDB;
	
	private final NodeIDType myID;
	
	private final JSONMessenger<NodeIDType> messenger;
	
	public TriggerProcessing(NodeIDType myID, HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> 
						subspaceInfoMap , HyperspaceMySQLDB<NodeIDType> hyperspaceDB, 
						JSONMessenger<NodeIDType> messenger )
	{
		this.myID = myID;
		this.messenger = messenger;
		this.hyperspaceDB = hyperspaceDB;
		
		ContextServiceLogger.getLogger().fine("generateSubspacePartitions completed");
	

		new Thread( new DeleteExpiredSearchesThread<NodeIDType>(subspaceInfoMap, myID, hyperspaceDB) ).start();
	}
	
	public boolean processTriggerOnQueryMsgFromUser(QueryInfo<NodeIDType> currReq)
	{
		String groupGUID = currReq.getGroupGUID();
		String userIP = currReq.getUserIP();
		int userPort = currReq.getUserPort();
		boolean found = false;
		
		try
		{
			found = this.hyperspaceDB.checkAndInsertSearchQueryRecordFromPrimaryTriggerSubspace
					(groupGUID, userIP, userPort);
			
			ContextServiceLogger.getLogger().fine(" search query "+currReq.getQuery()+" found "+found
					+" groupGUID "+groupGUID+" userIP "+userIP+" userPort "+userPort);
		}
		catch( Exception ex )
		{
			ex.printStackTrace();
		}
		return found;
	}
	
	public void processQuerySubspaceRegionMessageForTrigger
				( QueryMesgToSubspaceRegion<NodeIDType> queryMesgToSubspaceRegion )
	{
		String query 		= queryMesgToSubspaceRegion.getQuery();
		String groupGUID 	= queryMesgToSubspaceRegion.getGroupGUID();
		int subspaceId 		= queryMesgToSubspaceRegion.getSubspaceNum();
		String userIP       = queryMesgToSubspaceRegion.getUserIP();
		int userPort        = queryMesgToSubspaceRegion.getUserPort();
		long expiryTime		= queryMesgToSubspaceRegion.getExpiryTime();
		
		if( ContextServiceConfig.TRIGGER_ENABLED )
		{
			long expiryTimeFromNow = System.currentTimeMillis() + expiryTime;
			this.hyperspaceDB.insertIntoSubspaceTriggerDataInfo( subspaceId, 
					query, groupGUID, userIP, userPort, expiryTimeFromNow);
		}
	}
	
	public void processTriggerForValueUpdateToSubspaceRegion
		(ValueUpdateToSubspaceRegionMessage<NodeIDType> 
		valueUpdateToSubspaceRegionMessage, HashMap<String, GroupGUIDInfoClass> removedGroups, 
		HashMap<String, GroupGUIDInfoClass> addedGroups ) throws InterruptedException
	{
		int subspaceId  = valueUpdateToSubspaceRegionMessage.getSubspaceNum();
		JSONObject oldValJSON = valueUpdateToSubspaceRegionMessage.getOldValJSON();
		JSONObject newJSONToWrite = valueUpdateToSubspaceRegionMessage.getJSONToWrite();
		int requestType = valueUpdateToSubspaceRegionMessage.getOperType();
		JSONObject newUnsetAttr = valueUpdateToSubspaceRegionMessage.getNewUnsetAttrs();
		boolean firstTimeInsert = valueUpdateToSubspaceRegionMessage.getFirstTimeInsert();
		
		
		this.hyperspaceDB.getTriggerDataInfo(subspaceId, 
				oldValJSON, newJSONToWrite, 
				removedGroups, 
				addedGroups, 
				requestType, newUnsetAttr, firstTimeInsert); 
	}
	
	public void sendOutAggregatedRefreshTrigger
				( HashMap<String, GroupGUIDInfoClass> removedGroups, 
				HashMap<String, GroupGUIDInfoClass> addedGroups, String updateGUID, 
				long versionNum,
				long updateStartTime)
							throws JSONException
	{
		HashMap<String, JSONArray> sameClientRemovedTrigger 
												= new HashMap<String, JSONArray>();
		HashMap<String, JSONArray> sameClientAddedTrigger 
												= new HashMap<String, JSONArray>();
		
		Iterator<String> removedIter = removedGroups.keySet().iterator();
		while( removedIter.hasNext() )
		{
			String groupGUID = removedIter.next();
			GroupGUIDInfoClass groupInfo = removedGroups.get(groupGUID);
			String ipPortKey = groupInfo.getUserIP()+":"+groupInfo.getUserPort();
			
			JSONArray removedGroupGUIDArray = sameClientRemovedTrigger.get(ipPortKey);
			
			if( removedGroupGUIDArray == null )
			{
				removedGroupGUIDArray = new JSONArray();
				removedGroupGUIDArray.put(groupGUID);
				sameClientRemovedTrigger.put(ipPortKey, removedGroupGUIDArray);
			}
			else
			{
				removedGroupGUIDArray.put(groupGUID);
			}
		}
		
		
		Iterator<String> addedIter = addedGroups.keySet().iterator();
		while( addedIter.hasNext() )
		{
			String groupGUID = addedIter.next();
			GroupGUIDInfoClass groupInfo = addedGroups.get(groupGUID);
			String ipPortKey = groupInfo.getUserIP()+":"+groupInfo.getUserPort();
			
			JSONArray addedGroupGUIDArray = sameClientAddedTrigger.get(ipPortKey);
			
			if( addedGroupGUIDArray == null )
			{
				addedGroupGUIDArray = new JSONArray();
				addedGroupGUIDArray.put(groupGUID);
				sameClientAddedTrigger.put(ipPortKey, addedGroupGUIDArray);
			}
			else
			{
				addedGroupGUIDArray.put(groupGUID);
			}
		}
		
		Iterator<String> sameClientIter = sameClientRemovedTrigger.keySet().iterator();
		
		while( sameClientIter.hasNext() )
		{
			String ipPort = sameClientIter.next();
			JSONArray toBeRemovedGroupGUIDs = sameClientRemovedTrigger.get(ipPort);
			JSONArray toBeAddedGroupGUIDs = sameClientAddedTrigger.remove(ipPort);
			
			
			RefreshTrigger<NodeIDType> refTrig 
			= new RefreshTrigger<NodeIDType>
			(myID, toBeRemovedGroupGUIDs, 
					(toBeAddedGroupGUIDs!=null)?toBeAddedGroupGUIDs:new JSONArray(),
					versionNum, updateGUID, updateStartTime);
			
			String[] parsed = ipPort.split(":");
			
			String userIP 	= parsed[0];
			int userPort  	= Integer.parseInt(parsed[1]);
			
			ContextServiceLogger.getLogger().fine("processUpdateTriggerReply removed grps "
					+" userIP "+userIP+" userPort "+userPort);
			
			try
			{
				this.messenger.sendToAddress( new InetSocketAddress(userIP, userPort), 
						refTrig.toJSONObject() );
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		
		// sending for remaining
		sameClientIter = sameClientAddedTrigger.keySet().iterator();
		while( sameClientIter.hasNext() )
		{
			String ipPort = sameClientIter.next();
			
			RefreshTrigger<NodeIDType> refTrig 
			= new RefreshTrigger<NodeIDType>
			(myID, new JSONArray(), 
					sameClientAddedTrigger.get(ipPort),
					versionNum, updateGUID, updateStartTime);			

			
			String[] parsed = ipPort.split(":");
			
			String userIP 	= parsed[0];
			int userPort  	= Integer.parseInt(parsed[1]);
			
			ContextServiceLogger.getLogger().fine("processUpdateTriggerReply removed grps "
					+" userIP "+userIP+" userPort "+userPort);
			
			try
			{
				this.messenger.sendToAddress( new InetSocketAddress(userIP, userPort), 
						refTrig.toJSONObject() );
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
	}
}