package edu.umass.cs.contextservice.updates;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.HyperspaceMySQLDB;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;

public class UpdateInfo<NodeIDType>
{
	private final ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS;
	private final long updateRequestId;
	
	int numReplyRecvd;
	
	private boolean updateReqCompl;
	
	// string key is of the form subspaceId-replicaNum, value integer is 
	// num of replies
	private HashMap<String, Integer> hyperspaceHashingReplies 									= null;
	// counter over number of subspaces
	private int numRepliesCounter 																= 0;
	
	private final Object regionalRepliesLock 													= new Object();
	
	private int valueTriggerReplyCounter														= 0;
	
	private final Object triggerRepliesLock 													= new Object();
	
	// indexed by groupGUID
	private HashMap<String, JSONObject> toBeRemovedGroupsMap									= null;
	
	private HashMap<String, JSONObject> toBeAddedGroupsMap										= null;
	
	private HashMap<String, Integer> triggerReplyCounter 										= null;
	
	public UpdateInfo(ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS, long updateRequestId, 
			HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap)
	{
		this.valUpdMsgFromGNS = valUpdMsgFromGNS;
		this.updateRequestId = updateRequestId;
		numReplyRecvd = 0;
		//contextStartTime = System.currentTimeMillis();
		//updateStartTime = valUpdMsgFromGNS.getUpdateStartTime();
		
		updateReqCompl = false;
		
		hyperspaceHashingReplies = new HashMap<String, Integer>();
		
		if( ContextServiceConfig.TRIGGER_ENABLED )
		{
			triggerReplyCounter = new HashMap<String, Integer>();
			toBeRemovedGroupsMap = new HashMap<String, JSONObject>();
			toBeAddedGroupsMap = new HashMap<String, JSONObject>();
			
			JSONObject attrValuePairs = valUpdMsgFromGNS.getAttrValuePairs();
	
			// initilizating reply set
			Iterator<String> attrIter = attrValuePairs.keys();
			while(attrIter.hasNext())
			{
				String attrName = attrIter.next();
				
				// initialize updates
				Iterator<Integer> keyIter = subspaceInfoMap.keySet().iterator();
				
				while( keyIter.hasNext() )
				{
					int subspaceId = keyIter.next();
					Vector<SubspaceInfo<NodeIDType>> replicaVector = subspaceInfoMap.get(subspaceId);
			
					for( int i=0; i<replicaVector.size(); i++ )
					{
						SubspaceInfo<NodeIDType> currSubspaceReplica = replicaVector.get(i);
						if(currSubspaceReplica.getAttributesOfSubspace().containsKey(attrName))
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
		
		// initialize updates
		Iterator<Integer> keyIter = subspaceInfoMap.keySet().iterator();
		
		while( keyIter.hasNext() )
		{
			int subspaceId = keyIter.next();
			Vector<SubspaceInfo<NodeIDType>> replicaVector = subspaceInfoMap.get(subspaceId);
			
			for( int i=0; i<replicaVector.size(); i++ )
			{
				SubspaceInfo<NodeIDType> currSubspaceReplica = replicaVector.get(i);
				this.initializeSubspaceEntry(subspaceId, currSubspaceReplica.getReplicaNum());
			}
		}
	}
	
	public long getRequestId()
	{
		return updateRequestId;
	}
	
	public synchronized void incrementNumReplyRecvd()
	{
		this.numReplyRecvd++;
	}
	
	public int getNumReplyRecvd()
	{
		return this.numReplyRecvd;
	}
	
	public ValueUpdateFromGNS<NodeIDType> getValueUpdateFromGNS()
	{
		return this.valUpdMsgFromGNS;
	}
	
	public boolean getUpdComl()
	{
		return this.updateReqCompl;
	}
	
	public void  setUpdCompl()
	{
		this.updateReqCompl = true;
	}
	
	private void initializeSubspaceEntry(int subspaceId, int replicaNum)
	{
		hyperspaceHashingReplies.put(subspaceId+"-"+replicaNum, 0);
	}
	
	public boolean setUpdateReply( int subspaceId, int replicaNum, int numRep )
	{
		synchronized(this.regionalRepliesLock)
		{
			String mapKey = subspaceId+"-"+replicaNum;
			int repliesRecvdSoFar = this.hyperspaceHashingReplies.get(mapKey);
			repliesRecvdSoFar++;
			this.hyperspaceHashingReplies.put(mapKey, repliesRecvdSoFar);
			if(repliesRecvdSoFar == numRep)
			{
				this.numRepliesCounter++;
			}
			
			if( numRepliesCounter == this.hyperspaceHashingReplies.size() )
			{
				return true;
			}
			else
			{
				return false;
			}
		}
	}
	
	public boolean setUpdateTriggerReply(JSONArray toBeRemovedGroups, JSONArray toBeAddedGroups)
	{
		synchronized(triggerRepliesLock)
		{
			for(int i=0; i<toBeRemovedGroups.length();i++)
			{
				try 
				{
					JSONObject currGroup = toBeRemovedGroups.getJSONObject(i);
					//tableRow.put( "groupGUID", rs.getString("groupGUID") );
					String groupGUID = currGroup.getString(HyperspaceMySQLDB.groupGUID);
					toBeRemovedGroupsMap.put(groupGUID, currGroup);
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
					toBeAddedGroupsMap.put(groupGUID, currGroup);
				} catch (JSONException e)
				{
					e.printStackTrace();
				}
			}		
			valueTriggerReplyCounter++;
			ContextServiceLogger.getLogger().finest("valueTriggerReplyCounter "+valueTriggerReplyCounter +" for compl "
					+ (valUpdMsgFromGNS.getAttrValuePairs().length()*2*
							this.triggerReplyCounter.size() ) );
			// twice because reply comes from old and new value both
			// it can be just empty, but a reply always comes
			// two reply for each attribute from the replicated subsapces.
			// it can be optimized to reduce 2 replies to 1 but that is a TODO
			if(valueTriggerReplyCounter == (valUpdMsgFromGNS.getAttrValuePairs().length()*2*
					this.triggerReplyCounter.size() ) )
			{
				return true;
			}
			else
			{
				return false;
			}
		}
	}
	
	public boolean checkAllTriggerRepRecvd()
	{
		synchronized(triggerRepliesLock)
		{
			// twice because reply comes from old and new value both
			// it can be just empty, but a reply always comes
			// two reply for each attribute from the replicated subsapces.
			// it can be optimized to reduce 2 replies to 1 but that is a TODO
			if(valueTriggerReplyCounter == (valUpdMsgFromGNS.getAttrValuePairs().length()*2*
					this.triggerReplyCounter.size() ) )
			{
				return true;
			}
			else
			{
				return false;
			}
		}
	}
	
	public boolean checkAllUpdateReplyRecvd()
	{
		synchronized(this.regionalRepliesLock)
		{	
			if( numRepliesCounter == this.hyperspaceHashingReplies.size() )
			{
				return true;
			}
			else
			{
				return false;
			}
		}
	}
	
	public HashMap<String, JSONObject> getToBeRemovedGroups()
	{
		return this.toBeRemovedGroupsMap;
	}
	
	public HashMap<String, JSONObject> getToBeAddedGroups()
	{
		return this.toBeAddedGroupsMap;
	}
}