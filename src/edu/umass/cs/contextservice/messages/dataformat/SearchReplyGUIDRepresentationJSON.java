package edu.umass.cs.contextservice.messages.dataformat;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Represents the format for search reply guid format.
 * In privacy case, key is the anonymizedID and the value is
 * JSONArray of realID mapping to decrypt the anonyzmiedID to realID.
 * @author adipc
 */
public class SearchReplyGUIDRepresentationJSON 
{
	private enum Keys {ID, ANONYMIZEDID_TO_GUID_MAPPING};
	
	//TODO: sometime we want to change it to byte[] and save 
	// half space.
	private final String idString;
	// this can be empty or set to null if not used in no privacy case
	private JSONArray anonymizedIDToGUIDMapping;
	
	// no privacy constructor
	public SearchReplyGUIDRepresentationJSON(String GUID)
	{
		this.idString = GUID;
		this.anonymizedIDToGUIDMapping = null;
	}
	
	// privacy constructor
	public SearchReplyGUIDRepresentationJSON(String idString, 
			JSONArray anonymizedIDToGUIDMapping)
	{
		this.idString = idString;
		this.anonymizedIDToGUIDMapping = anonymizedIDToGUIDMapping;
	}
	
	public String getID()
	{
		return this.idString;
	}
	
	public JSONArray getAnonymizedIDToGuidMapping()
	{
		return this.anonymizedIDToGUIDMapping;
	}
	
	public JSONObject toJSONObject() throws JSONException
	{
		JSONObject jsonObject = new JSONObject();
		jsonObject.put(Keys.ID.toString(), idString);
		if(anonymizedIDToGUIDMapping != null)
		{
			jsonObject.put(Keys.ANONYMIZEDID_TO_GUID_MAPPING.toString(), 
					anonymizedIDToGUIDMapping);
		}
		return jsonObject;
	}
	
	public static SearchReplyGUIDRepresentationJSON fromJSONObject(JSONObject jsonObject) 
																		throws JSONException
	{
		String idString = jsonObject.getString(Keys.ID.toString());
		JSONArray realIDMappingInfo = null;
		if( jsonObject.has(Keys.ANONYMIZEDID_TO_GUID_MAPPING.toString()) )
		{
			realIDMappingInfo = jsonObject.getJSONArray
					(Keys.ANONYMIZEDID_TO_GUID_MAPPING.toString());
		}
		return new SearchReplyGUIDRepresentationJSON(idString, realIDMappingInfo);
	}
}