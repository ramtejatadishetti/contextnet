package edu.umass.cs.contextservice.schemes;

import org.json.JSONObject;
import edu.umass.cs.contextservice.messages.ACLUpdateToSubspaceRegionMessage;

/**
 * Defines methods for privacy processing 
 * for an update.
 * @author adipc
 */
public interface PrivacyProcessingInterface<NodeIDType>
{
	public void privacyProcessingOnUpdate( NodeIDType toSendNodeID , String nodeGUID , 
		   int subspaceId , long requestID , JSONObject updateAttrValuePairs );
	
	public void processACLUpdateToSubspaceRegionMessage(
			ACLUpdateToSubspaceRegionMessage<NodeIDType> aclUpdateToSubspaceRegionMessage, 
			int replicaNum );
}