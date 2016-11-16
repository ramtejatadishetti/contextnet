package edu.umass.cs.contextservice.schemes.components;


import org.json.JSONArray;

import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegion;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegionReply;
import edu.umass.cs.contextservice.messages.ValueUpdateToSubspaceRegionMessage;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.updates.UpdateInfo;

/**
 * Defnies the interface to process the updates and search
 * over GUID attribute value pairs. Basic hyperspace hashing mechanism.
 * @author adipc
 *
 */
public interface GUIDAttrValueProcessingInterfaceDele
{
	public void processQueryMsgFromUser
		(QueryInfo queryInfo, boolean storeQueryForTrigger);
	
	public int processQueryMesgToSubspaceRegion( QueryMesgToSubspaceRegion 
		queryMesgToSubspaceRegion, JSONArray resultGUIDArray );
	
	public void processQueryMesgToSubspaceRegionReply( QueryMesgToSubspaceRegionReply 
					queryMesgToSubspaceRegionReply );
	
	public int processValueUpdateToSubspaceRegionMessage( 
			ValueUpdateToSubspaceRegionMessage 
								valueUpdateToSubspaceRegionMessage, int replicaNum );
	
//	public void guidValueProcessingOnUpdate(
//			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo , 
//			JSONObject oldValueJSON , int subspaceId , int  replicaNum ,
//			JSONObject updatedAttrValJSON , String GUID , long requestID , 
//			boolean firstTimeInsert, long updateStartTime, 
//			JSONObject primarySubspaceJSON ) throws JSONException;
	
	public void processUpdateFromGNS(UpdateInfo updateReq);
}