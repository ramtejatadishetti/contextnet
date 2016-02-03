package edu.umass.cs.contextservice.updates;

import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.HyperspaceMySQLDB;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;

public class UpdateInfo<NodeIDType>
{
	private final ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS;
	private final long updateRequestId;
	
	// just for debugging the large update times.
	//private final long updateStartTime;
	//private final long contextStartTime;
	
	// stores the replies recvd from the value nodes for the query
	// Hash map indexed by componentId, and Vector<String> stores 
	// the GUIDs
	//public final HashMap<Integer, LinkedList<String>> repliesMap;
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
	
	
	public UpdateInfo(ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS, long updateRequestId)
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
			toBeRemovedGroupsMap = new HashMap<String, JSONObject>();
			toBeAddedGroupsMap = new HashMap<String, JSONObject>();
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
	
	public void initializeSubspaceEntry(int subspaceId, int replicaNum)
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
			
			/*if( numRepliesCounter == this.hyperspaceHashingReplies.size() )
			{
				return true;
			}
			else
			{
				return false;
			}*/
			
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
			
			// twice because reply comes from old and new value both
			// it can be just empty, but a reply always comes
			if(valueTriggerReplyCounter == (valUpdMsgFromGNS.getAttrValuePairs().length()*2) )
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
			if(valueTriggerReplyCounter == (valUpdMsgFromGNS.getAttrValuePairs().length()*2) )
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