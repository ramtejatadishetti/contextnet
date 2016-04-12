package edu.umass.cs.contextservice.messages.dataformat;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Defines the representation of value of an 
 * attribute.
 * 
 * @author adipc
 */
public class AttrValueRepresentationJSON
{
//	This class can also be used if we do encryption of values
//	in privacy scheme
	
	private enum Keys {VALUE, REAL_ID_MAPPING_JSONARAAY};
	
	private final String actualAttrValue;
	
	// this can be empty or set to null if not used in no privacy case
	// JSONArray of the encryption of real GUID with ACL member public keys.
	private JSONArray realIDMappingInfo = null;
	
	
	public AttrValueRepresentationJSON(String actualAttrValue)
	{
		this.actualAttrValue = actualAttrValue;
	}
	
	public AttrValueRepresentationJSON(String actualAttrValue, 
			JSONArray realIDMappingInfo)
	{
		this.actualAttrValue = actualAttrValue;
		this.realIDMappingInfo = realIDMappingInfo;
	}
	
	public String getActualAttrValue()
	{
		return this.actualAttrValue;
	}
	
	public JSONArray getRealIDMappingInfo()
	{
		return this.realIDMappingInfo;
	}
	
	public void setRealIDMappingInfo(JSONArray realIDMappingInfo)
	{
		this.realIDMappingInfo = realIDMappingInfo;
	}
	
	public JSONObject toJSONObject() throws JSONException
	{
		JSONObject jsonObject = new JSONObject();
		jsonObject.put(Keys.VALUE.toString(), actualAttrValue);
		if(realIDMappingInfo != null)
			jsonObject.put(Keys.REAL_ID_MAPPING_JSONARAAY.toString(), realIDMappingInfo);
		
		return jsonObject;
	}
	
	public static AttrValueRepresentationJSON fromJSONObject(JSONObject jsonObject) 
			throws JSONException
	{
		String actualAttrValue = jsonObject.getString(Keys.VALUE.toString());
		JSONArray realIDMappingInfo = null;
		if( jsonObject.has(Keys.REAL_ID_MAPPING_JSONARAAY.toString()) )
		{
			realIDMappingInfo 
				= jsonObject.getJSONArray(Keys.REAL_ID_MAPPING_JSONARAAY.toString());
		}
		return new AttrValueRepresentationJSON( actualAttrValue, realIDMappingInfo );
	}
	
	public String toString()
	{
		String str = "Value: "+actualAttrValue;
		if(realIDMappingInfo != null)
		{
			str= str+" realIDMappingInfo length "+realIDMappingInfo.length()+" realIDMappingInfo "+realIDMappingInfo;
		}
		return str;
	}
}