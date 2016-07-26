package edu.umass.cs.contextservice.schemes;

import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegion;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegionReply;
import edu.umass.cs.contextservice.messages.ValueUpdateToSubspaceRegionMessage;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;

/**
 * Defnies the interface to process the updates and search
 * over GUID attribute value pairs. Basic hyperspace hashing mechanism.
 * @author adipc
 *
 */
public interface GUIDAttrValueProcessingInterface<NodeIDType>
{
	public void processQueryMsgFromUser
		(QueryInfo<NodeIDType> queryInfo, boolean storeQueryForTrigger);
	
	public int processQueryMesgToSubspaceRegion( QueryMesgToSubspaceRegion<NodeIDType> 
		queryMesgToSubspaceRegion, JSONArray resultGUIDArray );
	
	public void processQueryMesgToSubspaceRegionReply( QueryMesgToSubspaceRegionReply<NodeIDType> 
					queryMesgToSubspaceRegionReply );
	
	public int processValueUpdateToSubspaceRegionMessage( 
			ValueUpdateToSubspaceRegionMessage<NodeIDType> 
								valueUpdateToSubspaceRegionMessage, int replicaNum );
	
	public void guidValueProcessingOnUpdate(
			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo , 
			JSONObject oldValueJSON , int subspaceId , int  replicaNum ,
			JSONObject updatedAttrValJSON , String GUID , long requestID , 
			boolean firstTimeInsert, long updateStartTime, 
			JSONObject primarySubspaceJSON ) throws JSONException;
}