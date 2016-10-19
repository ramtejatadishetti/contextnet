package edu.umass.cs.contextservice.client.csprivacytransform;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * This message stores  the anonymized ID, attrValuePairs,  
 * the mapping information to real GUID for each attribute 
 * associated to this anonymized ID.
 * @author adipc
 */
public class CSUpdateTransformedMessage
{
	private final String anonymizedIDString;
	private final JSONObject csAttrValPairs;
	
	private final JSONArray anonymizedIDToGuidMapping;
	
	private final JSONArray anonymizedIDAttrSet;
	
	public CSUpdateTransformedMessage( String anonymizedIDString, 
			JSONObject csAttrValPairs, JSONArray anonymizedIDToGuidMapping,
			JSONArray anonymizedIDAttrSet )
	{
		this.anonymizedIDString = anonymizedIDString;
		this.csAttrValPairs = csAttrValPairs;
		this.anonymizedIDToGuidMapping = anonymizedIDToGuidMapping;
		this.anonymizedIDAttrSet = anonymizedIDAttrSet;
	}
	
	public String getAnonymizedIDString()
	{
		return this.anonymizedIDString;
	}
	
	public JSONObject getAttrValJSON()
	{
		return this.csAttrValPairs;
	}
	
	public JSONArray getAnonymizedIDToGuidMapping()
	{
		return anonymizedIDToGuidMapping;
	}
	
	public JSONArray getAnonymizedIDAttrSet()
	{
		return anonymizedIDAttrSet;
	}
	
	public String toString()
	{
		String str="anonymized ID "+anonymizedIDString+" csAttrValPairs "
					+csAttrValPairs;
		
		if( anonymizedIDToGuidMapping != null )
		{
			str = str+ "anonymizedIDToGuidMapping "
						+ anonymizedIDToGuidMapping.toString();
		}
		return str;
	}
}