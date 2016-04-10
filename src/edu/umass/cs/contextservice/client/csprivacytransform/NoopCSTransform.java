package edu.umass.cs.contextservice.client.csprivacytransform;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;


import org.json.JSONObject;

import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.utils.Utils;

public class NoopCSTransform implements CSPrivacyTransformInterface
{
	
	@Override
	public List<CSTransformedMessage> transformUpdateForCSPrivacy(String targetGuid, JSONObject attrValuePairs,
			HashMap<String, List<ACLEntry>> aclMap, List<AnonymizedIDEntry> anonymizedIDList) 
	{
		CSTransformedMessage csTransformedMessage 
				= new CSTransformedMessage(Utils.hexStringToByteArray(targetGuid), 
						attrValuePairs, new JSONObject());
		List<CSTransformedMessage> returnList = new LinkedList<CSTransformedMessage>();
		returnList.add(csTransformedMessage);
		return returnList;
	}

	@Override
	public List<String> unTransformSearchReply(List<CSTransformedMessage> csTransformedList) 
	{
		List<String> resultGUIDs = new LinkedList<String>();
		for(int i=0;i<csTransformedList.size(); i++)
		{
			resultGUIDs.add(Utils.bytArrayToHex(csTransformedList.get(i).getAnonymizedID()));
		}
		return resultGUIDs;
	}
}