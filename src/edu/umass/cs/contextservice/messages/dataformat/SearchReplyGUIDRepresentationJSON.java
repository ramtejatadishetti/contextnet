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
	private enum Keys {ID, REAL_ID_MAPPING_JSONARAAY};
	
	//TODO: sometime we want to change it to byte[] and save 
	// half space.
	private final String idString;
	// this can be empty or set to null if not used in no privacy case
	private JSONArray realIDMappingInfo;
	
	// no privacy constructor
	public SearchReplyGUIDRepresentationJSON(String GUID)
	{
		this.idString = GUID;
		this.realIDMappingInfo = null;
	}
	
	// privacy constructor
	public SearchReplyGUIDRepresentationJSON(String idString, 
			JSONArray realIDMappingInfo)
	{
		this.idString = idString;
		this.realIDMappingInfo = realIDMappingInfo;
	}
	
	public String getID()
	{
		return this.idString;
	}
	
	public JSONArray getRealIDMappingInfo()
	{
		return this.realIDMappingInfo;
	}
	
	public JSONObject toJSONObject() throws JSONException
	{
		JSONObject jsonObject = new JSONObject();
		jsonObject.put(Keys.ID.toString(), idString);
		if(realIDMappingInfo != null)
		{
			jsonObject.put(Keys.REAL_ID_MAPPING_JSONARAAY.toString(), realIDMappingInfo);
		}
		return jsonObject;
	}
	
	public static SearchReplyGUIDRepresentationJSON fromJSONObject(JSONObject jsonObject) throws JSONException
	{
		String idString = jsonObject.getString(Keys.ID.toString());
		JSONArray realIDMappingInfo = null;
		if( jsonObject.has(Keys.REAL_ID_MAPPING_JSONARAAY.toString()) )
		{
			realIDMappingInfo = jsonObject.getJSONArray
					(Keys.REAL_ID_MAPPING_JSONARAAY.toString());
		}
		return new SearchReplyGUIDRepresentationJSON(idString, realIDMappingInfo);
	}
}