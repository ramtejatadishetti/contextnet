package edu.umass.cs.contextservice.updates;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.UpdateTriggerReply;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;

public class UpdateInfo<NodeIDType>
{
	// for value update reply from subspace
	//public static final int VALUE_UPDATE_REPLY									= 1;
	// for privacy update reply from subspace
	//public static final int PRIVACY_UPDATE_REPLY								= 2;
	
	
	private final ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS;
	
	private final long updateRequestId;
	
	private boolean updateReqCompl;
	
	// string key is of the form subspaceId-replicaNum, value integer is 
	// num of replies
	private HashMap<String, Integer> valueUpdateRepliesMap 						= null;
	// counter over number of subspaces
	private int valueUpdateRepliesCounter 										= 0;
	
	
	// string key is of the form subspaceId-replicaNum, value integer is 
	// num of replies
	//private HashMap<String, Integer> privacyRepliesMap 						= null;
	// counter over number of subspaces
	// FIXME: privacy update in anonymized ID later can be optimized 
	// so that the anonymized ID is only updated in subspaces whose attributes
	// have a non-zero intersection with attribute set of the anonymized ID.
	// But for now it is updated in every subspace.
	//private int privacyRepliesCounter 											= 0;
	
	
	private final Object subspaceRepliesLock 									= new Object();
	
	
	private final Object triggerRepliesLock 									= new Object();
	
	
	private HashMap<String, UpdateTriggerInfo<NodeIDType>>	attrKeyTriggerInfo	= null;
	
	private int numAttrsTriggerCompl											= 0;
	
	
	public UpdateInfo( ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS, long updateRequestId, 
			HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap )
	{
		this.valUpdMsgFromGNS = valUpdMsgFromGNS;
		this.updateRequestId  = updateRequestId;
		
		updateReqCompl = false;
		
		valueUpdateRepliesMap = new HashMap<String, Integer>();
		
		
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
		if(subspaceInfoMap != null)
		{
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
	}
	
	public long getRequestId()
	{
		return updateRequestId;
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
		valueUpdateRepliesMap.put(subspaceId+"-"+replicaNum, 0);
	}
	
	public boolean setUpdateReply( int subspaceId, int replicaNum, int numRep)
	{
		//if( updateType == VALUE_UPDATE_REPLY )
		{
			synchronized( this.subspaceRepliesLock )
			{
				String mapKey = subspaceId+"-"+replicaNum;
				int repliesRecvdSoFar = this.valueUpdateRepliesMap.get(mapKey);
				repliesRecvdSoFar++;
				this.valueUpdateRepliesMap.put(mapKey, repliesRecvdSoFar);
				
				if( repliesRecvdSoFar == numRep )
				{
					this.valueUpdateRepliesCounter++;
				}
				
				if( valueUpdateRepliesCounter == this.valueUpdateRepliesMap.size() )
				{
					return true;
					// if privacy replies are recvd from all subspaces
//					if( !ContextServiceConfig.PRIVACY_ENABLED ||
//							privacyRepliesCounter == this.valueUpdateRepliesMap.size() )
//					{
//						return true;
//					}
//					else
//					{
//						return false;
//					}
				}
				else
				{
					return false;
				}
			}
		}
//		else if( updateType == PRIVACY_UPDATE_REPLY )
//		{
//			synchronized( this.subspaceRepliesLock )
//			{
//				privacyRepliesCounter++;			
//				if( valueUpdateRepliesCounter == this.valueUpdateRepliesMap.size() )
//				{
//					// if privacy replies are recvd from all subspaces
//					if( privacyRepliesCounter == this.valueUpdateRepliesMap.size() )
//					{
//						return true;
//					}
//					else
//					{
//						return false;
//					}
//				}
//				else
//				{
//					return false;
//				}
//			}
//		}
//		assert(false);
//		return false;
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
		synchronized(this.subspaceRepliesLock)
		{
			if( valueUpdateRepliesCounter == this.valueUpdateRepliesMap.size() )
			{
				return true;
				// if privacy replies are recvd from all subspaces
//				if( !ContextServiceConfig.PRIVACY_ENABLED ||
//						privacyRepliesCounter == this.valueUpdateRepliesMap.size() )
//				{
//					return true;
//				}
//				else
//				{
//					return false;
//				}
			}
			else
			{
				return false;
			}
			
//			if( numRepliesCounter == this.hyperspaceHashingReplies.size() )
//			{
//				return true;
//			}
//			else
//			{
//				return false;
//			}
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
	
//	public synchronized void incrementNumReplyRecvd()
//	{
//		this.numReplyRecvd++;
//	}
//	public int getNumReplyRecvd()
//	{
//		return this.numReplyRecvd;
//	}
}