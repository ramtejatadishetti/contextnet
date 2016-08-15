package edu.umass.cs.contextservice.updates;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.triggers.GroupGUIDInfoClass;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;

public class UpdateInfo<NodeIDType>
{	
	private final ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS;
	
	private final long updateRequestId;
	
	private boolean updateReqCompl;
	
	// string key is of the form subspaceId-replicaNum, value integer is 
	// num of replies
	private HashMap<String, Integer> valueUpdateRepliesMap 						= null;
	// counter over number of subspaces
	private int valueUpdateRepliesCounter 										= 0;
	
	
	private HashMap<String, GroupGUIDInfoClass> toBeRemovedMap;
	private HashMap<String, GroupGUIDInfoClass> toBeAddedMap;
	
	
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
	
	
	public UpdateInfo( ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS, long updateRequestId, 
			HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap )
	{
		this.valUpdMsgFromGNS = valUpdMsgFromGNS;
		this.updateRequestId  = updateRequestId;
		
		updateReqCompl = false;
		
		valueUpdateRepliesMap = new HashMap<String, Integer>();
		
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
		
		if( ContextServiceConfig.TRIGGER_ENABLED )
		{
			toBeRemovedMap = new HashMap<String, GroupGUIDInfoClass>();
			toBeAddedMap = new HashMap<String, GroupGUIDInfoClass>();
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
	
	private void initializeSubspaceEntry( int subspaceId, int replicaNum )
	{
		valueUpdateRepliesMap.put(subspaceId+"-"+replicaNum, 0);
	}
	
	public boolean setUpdateReply( int subspaceId, int replicaNum, int numRep,
			JSONArray toBeRemovedGroups, JSONArray toBeAddedGroups )
	{
		assert( toBeRemovedGroups != null );
		assert( toBeAddedGroups != null );
		
		synchronized( this.subspaceRepliesLock )
		{
			if( ContextServiceConfig.TRIGGER_ENABLED )
			{
				for( int i=0; i<toBeRemovedGroups.length(); i++ )
				{
					try 
					{
						GroupGUIDInfoClass groupGUIDInfo 
								= new GroupGUIDInfoClass(toBeRemovedGroups.getJSONObject(i));
						
						String groupGUID = groupGUIDInfo.getGroupGUID();
						
						// doing duplicate elimination right here.
						// as a query can span multiple nodes in a subspace.
						toBeRemovedMap.put(groupGUID, groupGUIDInfo);
					}
					catch (JSONException e)
					{
						e.printStackTrace();
					}
				}
				
				
				for( int i=0; i<toBeAddedGroups.length(); i++ )
				{
					GroupGUIDInfoClass groupGUIDInfo;
					try 
					{
						groupGUIDInfo 
							= new GroupGUIDInfoClass(toBeAddedGroups.getJSONObject(i));
						
						String groupGUID = groupGUIDInfo.getGroupGUID();
						
						// doing duplicate elimination right here.
						// as a query can span multiple nodes in a subspace.
						toBeAddedMap.put(groupGUID, groupGUIDInfo);
					} catch (JSONException e) 
					{
						e.printStackTrace();
					}
				}
			}
			
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
			}
			else
			{
				return false;
			}
		}
	}
	
	public HashMap<String, GroupGUIDInfoClass> getToBeRemovedMap()
	{
		return this.toBeRemovedMap;
	}
	
	public HashMap<String, GroupGUIDInfoClass> getToBeAddedMap()
	{
		return this.toBeAddedMap;
	}
}