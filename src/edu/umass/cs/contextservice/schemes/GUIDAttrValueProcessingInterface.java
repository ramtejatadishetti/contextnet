package edu.umass.cs.contextservice.schemes;

import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegion;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegionReply;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.ValueUpdateToSubspaceRegionMessage;
import edu.umass.cs.contextservice.messages.dataformat.AttrValueRepresentationJSON;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;

/**
 * Defnies the interface to process the updates and search
 * over GUID attribute value pairs. Basic hyperspace hashing mechanism.
 * @author adipc
 *
 */
public interface GUIDAttrValueProcessingInterface<NodeIDType>
{
	public QueryInfo<NodeIDType> processQueryMsgFromUser
		(QueryMsgFromUser<NodeIDType> queryMsgFromUser);
	
	public void processQueryMesgToSubspaceRegion(QueryMesgToSubspaceRegion<NodeIDType> 
		queryMesgToSubspaceRegion);
	
	public void processQueryMesgToSubspaceRegionReply(QueryMesgToSubspaceRegionReply<NodeIDType> 
					queryMesgToSubspaceRegionReply);
	
	public void processValueUpdateToSubspaceRegionMessage( 
			ValueUpdateToSubspaceRegionMessage<NodeIDType> 
								valueUpdateToSubspaceRegionMessage, int replicaNum );
	
	public void guidValueProcessingOnUpdate(
			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo , 
			JSONObject oldValueJSON , int subspaceId , int  replicaNum ,
			HashMap<String, AttrValueRepresentationJSON> attrValMap ,
			String GUID , long requestID , JSONObject attrValuePairs ,
			boolean firstTimeInsert) throws JSONException;
}