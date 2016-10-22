package edu.umass.cs.contextservice.client.csprivacytransform;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.contextservice.client.common.ACLEntry;

public class NoopCSTransform implements CSPrivacyTransformInterface
{
	@Override
	public List<CSUpdateTransformedMessage> transformUpdateForCSPrivacy(String targetGuid,
			JSONObject  attrValuePairs, HashMap<String, List<ACLEntry>> aclMap,
			List<AnonymizedIDEntry> anonymizedIDList) 
	{
		CSUpdateTransformedMessage csTransformedMessage 
			= new CSUpdateTransformedMessage(targetGuid, 
					attrValuePairs, null, null);
		List<CSUpdateTransformedMessage> returnList 
						= new LinkedList<CSUpdateTransformedMessage>();
		returnList.add(csTransformedMessage);
		return returnList;
	}
	
	@Override
	public void unTransformSearchReply(GuidEntry myGuid, 
			List<CSSearchReplyTransformedMessage> csTransformedList, 
			JSONArray replyArray) 
	{
		for(int i=0;i<csTransformedList.size(); i++)
		{
			replyArray.put(csTransformedList.get(i).getSearchGUIDObj().getID());
		}
	}

	@Override
	public void unTransformSearchReply(HashMap<String, byte[]> anonymizedIDToSecretKeyMap,
			List<CSSearchReplyTransformedMessage> csTransformedList, JSONArray replyArray) 
	{	
	}

	@Override
	public void unTransformSearchReply(GuidEntry myGuid, List<CSSearchReplyTransformedMessage> csTransformedList,
			HashMap<String, Boolean> replyMap) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void unTransformSearchReply(HashMap<String, byte[]> anonymizedIDToSecretKeyMap,
			List<CSSearchReplyTransformedMessage> csTransformedList, HashMap<String, Boolean> replyMap) {
		// TODO Auto-generated method stub
		
	}
}