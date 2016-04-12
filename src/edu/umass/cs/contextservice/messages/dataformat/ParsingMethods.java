package edu.umass.cs.contextservice.messages.dataformat;

import java.util.HashMap;
import java.util.Iterator;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Class that contains static parsing methods
 * @author adipc
 *
 */
public class ParsingMethods 
{
	/**
	 * this map is created once after receiving attr Value pair.
	 * 
	 * Creating this map once reduces the multiple parsing of attrValue JSON.
	 * which can be big on privacy case.
	 * @return
	 * @throws JSONException 
	 */
	public static HashMap<String, AttrValueRepresentationJSON> 
		getAttrValueMap(JSONObject attrValuePair) throws JSONException
	{
		HashMap<String, AttrValueRepresentationJSON> attrValueMap 
						= new HashMap<String, AttrValueRepresentationJSON>();
		Iterator<String> attrIter = attrValuePair.keys();
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			AttrValueRepresentationJSON attrValRep 
				=  AttrValueRepresentationJSON.fromJSONObject
				( attrValuePair.getJSONObject(attrName) );
			
			attrValueMap.put(attrName, attrValRep);
		}
		return attrValueMap;
	}
	
	public static JSONObject getJSONObject(
			HashMap<String, AttrValueRepresentationJSON> attrValMap) throws JSONException
	{
		JSONObject jsonObject = new JSONObject();
		Iterator<String> attrIter = attrValMap.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			jsonObject.put(attrName, attrValMap.get(attrName).toJSONObject());
		}
		return jsonObject;
	}
}