package edu.umass.cs.contextservice.schemes;

import java.io.IOException;
import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.database.HyperspaceMySQLDB;
import edu.umass.cs.contextservice.messages.ACLUpdateToSubspaceRegionMessage;
import edu.umass.cs.contextservice.messages.ACLUpdateToSubspaceRegionReplyMessage;
import edu.umass.cs.contextservice.messages.dataformat.AttrValueRepresentationJSON;
import edu.umass.cs.contextservice.messages.dataformat.ParsingMethods;
import edu.umass.cs.nio.JSONMessenger;

/**
 * 
 * @author adipc
 */
public class PrivacyProcessing<NodeIDType> implements PrivacyProcessingInterface<NodeIDType>
{
	private final NodeIDType myID;
	
	private final HyperspaceMySQLDB<NodeIDType> hyperspaceDB;
	
	private final JSONMessenger<NodeIDType> messenger;
	
	public PrivacyProcessing( NodeIDType myID , 
									HyperspaceMySQLDB<NodeIDType> hyperspaceDB , 
									JSONMessenger<NodeIDType> messenger )
	{
		this.myID = myID;
		this.hyperspaceDB = hyperspaceDB;
		this.messenger = messenger;
	}
	
	/**
	 * This function sends privacy update to the consistently hashed node 
	 * in each subspace.
	 */
	@Override
	public void privacyProcessingOnUpdate( NodeIDType toSendNodeID , String nodeGUID , 
			int subspaceId , long requestID , JSONObject updateAttrValuePairs )
	{
		// FIXME: need to check if there are two cases of add and udpate
		ACLUpdateToSubspaceRegionMessage<NodeIDType> aclUpdMesg 
				= new ACLUpdateToSubspaceRegionMessage<NodeIDType>
					( myID, -1, nodeGUID , 
						updateAttrValuePairs, ACLUpdateToSubspaceRegionMessage.ADD_ENTRY , 
						subspaceId , requestID );
		
		try
		{
			this.messenger.sendToID( toSendNodeID, aclUpdMesg.toJSONObject() );
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public void processACLUpdateToSubspaceRegionMessage(
			ACLUpdateToSubspaceRegionMessage<NodeIDType> aclUpdateToSubspaceRegionMessage, 
			int replicaNum )
	{
		try 
		{
			long requestID  = aclUpdateToSubspaceRegionMessage.getRequestID();
			long versionNum = aclUpdateToSubspaceRegionMessage.getVersionNum();
			String anonymizedID = aclUpdateToSubspaceRegionMessage.getGUID();
			JSONObject updValJSON = aclUpdateToSubspaceRegionMessage.getUpdateAttrValuePairs();
			int subspaceId 	= aclUpdateToSubspaceRegionMessage.getSubspaceNum(); 
			
			HashMap<String, AttrValueRepresentationJSON> atrToValueRep 
									= ParsingMethods.getAttrValueMap(updValJSON);
			
			this.hyperspaceDB.storePrivacyInformationOnUpdate(anonymizedID , 
					atrToValueRep , subspaceId);
			
			// update done, now send back the reply
			ACLUpdateToSubspaceRegionReplyMessage<NodeIDType> aclUpdReplyMesg 
				= new ACLUpdateToSubspaceRegionReplyMessage<NodeIDType>( myID, versionNum, 
					requestID, subspaceId, replicaNum );
			
			try
			{
				this.messenger.sendToID( aclUpdateToSubspaceRegionMessage.getSender(), 
						aclUpdReplyMesg.toJSONObject() );
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
}