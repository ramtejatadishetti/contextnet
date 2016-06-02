package edu.umass.cs.contextservice.client.csprivacytransform;

import java.util.HashMap;
import java.util.List;

import org.apache.commons.codec.DecoderException;
import org.json.JSONArray;
import org.json.JSONObject;

import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.contextservice.client.common.ACLEntry;

/**
 * Defines the interface for CS privacy transform
 * @author adipc
 */
public interface CSPrivacyTransformInterface 
{
	/**
	 * This function transforms an update into privacy preserving updates to CS.
	 * A single update to a GUID can result in multiple anonymized Ids of that guid being updated,
	 * which is returned as a list.
	 * @param targetGuid
	 * @param attrValueMap
	 * @param aclMap
	 * @param anonymizedIDList
	 * @param versionNum
	 * @return
	 * @throws DecoderException 
	 */
	public List<CSUpdateTransformedMessage> transformUpdateForCSPrivacy
				( String targetGuid, JSONObject attrValuePairs , 
						HashMap<String, List<ACLEntry>> aclMap, 
						List<AnonymizedIDEntry> anonymizedIDList ) throws DecoderException;
	
	/**
	 * Returns a list of search queries.
	 * All attributes in a search quer, in the list, belongs to 
	 * one subspace. Each search query returns anonymized IDs, those are then 
	 * conjuncted by the querier.
	 * @param userSearchQuery
	 * @param subspaceAttrMap
	 * @return
	 */
//	public List<String> transformSearchQueryForCSPrivacy( String userSearchQuery, 
//			HashMap<Integer, JSONArray> subspaceAttrMap );

	/**
	 * untransforms the result of anonymized IDs obtained in a search reply.
	 * returns a list of GUIDs in replyArray.
	 * This is the application specified replyArray, so result is directly returned in it.
	 * So, that no extra copying is done.
	 * @param csTransformedList
	 * @param replyArray
	 * @return
	 */
	public void unTransformSearchReply(GuidEntry myGuid, 
			List<CSSearchReplyTransformedMessage> csTransformedList, 
			JSONArray replyArray);
}