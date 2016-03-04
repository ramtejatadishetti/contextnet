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
import edu.umass.cs.contextservice.messages.UpdateTriggerReply;
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
	
	//private int valueTriggerReplyCounter														= 0;
	
	private final Object triggerRepliesLock 													= new Object();
	
//	// indexed by groupGUID
//	private HashMap<String, JSONObject> toBeRemovedGroupsMap									= null;
//	private HashMap<String, JSONObject> toBeAddedGroupsMap										= null;
	// String attrName, with number of replies to expect from replicated subspaces
	//private HashMap<String, Integer> triggerReplyCounter 										= null;
	//private int numberOfTriggerRepliesToExpect												= 0;
	// key is attrName
	private HashMap<String, UpdateTriggerInfo<NodeIDType>>	attrKeyTriggerInfo					= null;
	
	private int numAttrsTriggerCompl															= 0;

	
	
	public UpdateInfo( ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS, long updateRequestId, 
			HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap )
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
			attrKeyTriggerInfo = new HashMap<String, UpdateTriggerInfo<NodeIDType>>();
			
//			triggerReplyCounter = new HashMap<String, Integer>();
//			toBeRemovedGroupsMap = new HashMap<String, JSONObject>();
//			toBeAddedGroupsMap = new HashMap<String, JSONObject>();
			
			JSONObject attrValuePairs = valUpdMsgFromGNS.getAttrValuePairs();
	
			// initilizating reply set
			Iterator<String> attrIter = attrValuePairs.keys();
			while(attrIter.hasNext())
			{
				String attrName = attrIter.next();
				
				UpdateTriggerInfo<NodeIDType> attrUpdTriggerInfo = new UpdateTriggerInfo<NodeIDType>(attrName, subspaceInfoMap);
				attrKeyTriggerInfo.put(attrName, attrUpdTriggerInfo);	
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
	
	public boolean setUpdateTriggerReply(UpdateTriggerReply<NodeIDType> updateTriggerReply)
	{
		String attrName 				= updateTriggerReply.getAttrName();
		
		synchronized(triggerRepliesLock)
		{
			boolean compl = this.attrKeyTriggerInfo.get(attrName).addTriggerReply(updateTriggerReply);
//			System.out.println("Trigger arrvd  numAttrsTriggerCompl "
//					+numAttrsTriggerCompl +" for compl "+ this.attrKeyTriggerInfo.size() );
			if(compl)
			{
				numAttrsTriggerCompl++;
				
				// overall compl
				if(numAttrsTriggerCompl == this.attrKeyTriggerInfo.size() )
				{
					ContextServiceLogger.getLogger().fine("overall trigger compl numAttrsTriggerCompl "
				+numAttrsTriggerCompl +" for compl "+ this.attrKeyTriggerInfo.size() );
					
					
					return true;
				}
			}
			return false;
		}
	}
	
	
	public boolean checkAllTriggerRepRecvd()
	{
		synchronized(triggerRepliesLock)
		{
			if(numAttrsTriggerCompl == this.attrKeyTriggerInfo.size() )
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
	
	public JSONArray getRemovedGroupsForAttr(String attrName)
	{
		return this.attrKeyTriggerInfo.get(attrName).getRemovedTriggersForAttr();
	}
	
	public JSONArray getToBeAddedGroupsForAttr(String attrName)
	{
		return this.attrKeyTriggerInfo.get(attrName).getAddedTriggersForAttr();
	}
}