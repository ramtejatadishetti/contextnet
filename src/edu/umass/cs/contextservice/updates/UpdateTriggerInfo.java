package edu.umass.cs.contextservice.updates;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.database.HyperspaceMySQLDB;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.UpdateTriggerReply;

/**
 * stores the update trigger info for each attribute of an udpate
 * @author adipc
 *
 * @param <NodeIDType>
 */
public class UpdateTriggerInfo<NodeIDType>
{
	// attrName
	private final String attrName;
	private HashMap<String, JSONObject> oldValueGrpMap											= null;
	
	private HashMap<String, JSONObject> newValGrpMap											= null;
	
	private HashMap<String, Integer> triggerReplyCounter 										= null;
	
	private int numberOfTriggerReplicaCompl														= 0;
	
	private final HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap;
	
	// stores the final group guids
	private final JSONArray triggerRemovedGrpGUIDs;
	private final JSONArray triggerAddedGrpGUIDs;
	
	
	public UpdateTriggerInfo(String attrName,
			HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap)
	{
		this.attrName = attrName;
		this.subspaceInfoMap = subspaceInfoMap;
		oldValueGrpMap = new HashMap<String, JSONObject>();
		newValGrpMap = new HashMap<String, JSONObject>();
		triggerReplyCounter = new HashMap<String, Integer>();
		triggerRemovedGrpGUIDs = new JSONArray();
		triggerAddedGrpGUIDs = new JSONArray();
		
		initialize();
	}
	
	public boolean addTriggerReply(UpdateTriggerReply<NodeIDType> updateTriggerReply)
	{
		JSONArray toBeAddedGroups  		= updateTriggerReply.getToBeAddedGroups();
		JSONArray toBeRemovedGroups 	= updateTriggerReply.getToBeRemovedGroups();
		int subspaceId 					= updateTriggerReply.getSubspaceNum();
		int replicaNum 					= updateTriggerReply.getReplicaNum();
		int numReplies					= updateTriggerReply.getNumReplies();
		int oldNewOrBoth				= updateTriggerReply.getOldNewBoth();
		
		for(int i=0; i<toBeRemovedGroups.length();i++)
		{
			try 
			{
				JSONObject currGroup = toBeRemovedGroups.getJSONObject(i);
				//tableRow.put( "groupGUID", rs.getString("groupGUID") );
				String groupGUID = currGroup.getString(HyperspaceMySQLDB.groupGUID);
				oldValueGrpMap.put(groupGUID, currGroup);
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		for(int i=0; i<toBeAddedGroups.length(); i++)
		{
			try 
			{
				JSONObject currGroup = toBeAddedGroups.getJSONObject(i);
				//tableRow.put( "groupGUID", rs.getString("groupGUID") );
				String groupGUID = currGroup.getString(HyperspaceMySQLDB.groupGUID);
				newValGrpMap.put(groupGUID, currGroup);
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		int currReply = this.triggerReplyCounter.get(subspaceId+"-"+replicaNum);
		currReply++;
		this.triggerReplyCounter.put(subspaceId+"-"+replicaNum, currReply);
		
		// this replica compl
		if(currReply == numReplies)
		{
			this.numberOfTriggerReplicaCompl++;
		}
		
		if(numberOfTriggerReplicaCompl == this.triggerReplyCounter.size() )
		{
			// do the processing of addition and removals of groupGUIDs
			processCompletionOfAttributeTrigger();
			ContextServiceLogger.getLogger().fine("Update trigger for attr "+attrName+" complete "
					+" removed triggers size "+this.triggerRemovedGrpGUIDs.length()
					+" added triggers size "+this.triggerAddedGrpGUIDs.length());
			return true;
		}
		else
		{
			return false;
		}
	}
	
	public JSONArray getRemovedTriggersForAttr()
	{
		return this.triggerRemovedGrpGUIDs;
	}
	
	
	public JSONArray getAddedTriggersForAttr()
	{
		return this.triggerAddedGrpGUIDs;
	}
	
	private void processCompletionOfAttributeTrigger()
	{
		Iterator<String> grpGUIDIter = oldValueGrpMap.keySet().iterator();
		while( grpGUIDIter.hasNext() )
		//for(int i=0; i<toBeRemovedGroups.length();i++)
		{
			String currGrpGUID = grpGUIDIter.next();
			// if it is not in the new value grps then we need to send trigger
			if( !newValGrpMap.containsKey(currGrpGUID) )
			{
				this.triggerRemovedGrpGUIDs.put(oldValueGrpMap.get(currGrpGUID));
			}
		}
		
		grpGUIDIter = newValGrpMap.keySet().iterator();
		
		while( grpGUIDIter.hasNext() )
		{
			String currGrpGUID = grpGUIDIter.next();
			
			// if oldvlaue groups don't have it then we need to send a trigger
			if( !oldValueGrpMap.containsKey(currGrpGUID))
			{
				this.triggerAddedGrpGUIDs.put(newValGrpMap.get(currGrpGUID));
			}
		}
		
	}
	
	private void initialize()
	{
		// initialize updates
		Iterator<Integer> keyIter = subspaceInfoMap.keySet().iterator();
		
		while( keyIter.hasNext() )
		{
			int subspaceId = keyIter.next();
			Vector<SubspaceInfo<NodeIDType>> replicaVector = subspaceInfoMap.get(subspaceId);
	
			for( int i=0; i<replicaVector.size(); i++ )
			{
				SubspaceInfo<NodeIDType> currSubspaceReplica = replicaVector.get(i);
				if( currSubspaceReplica.getAttributesOfSubspace().containsKey(attrName) )
				{
					triggerReplyCounter.put(subspaceId+"-"+currSubspaceReplica.getReplicaNum(), 0);
				}
				else
				{
					// if not, then none of the replicas will have it.
					break;
				}
			}
		}
	}
	
}