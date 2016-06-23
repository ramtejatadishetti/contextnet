package edu.umass.cs.contextservice.client.common;

import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.utils.Utils;

/**
 * The class represents anonymized ID and its 
 * associated attributes(Bu,l) and guids(Gu,l).
 * @author adipc
 *
 */
public class AnonymizedIDEntry 
{	
	private final String anonymizedIDString;
	private final HashMap<String, Boolean> attributeMap;
	private final JSONArray guidSet;
	
	// this array is in order so that a search querier can do a quick lookup
	// to find its entry to decrypt.
	// Each element of GUID set is hashed on the length.
	// Each element in guidSet is stored corresponding to its hashing location.
	// If there is a collision, then it is stored on next available.
	// But advantage here is that as the list grows in size, collisions will be less. 
	// The querier doesn't need to know whole ACL, it just needs to know the size, which it would
	// as the querier gets whole anonymizedIDToGUIDMapping array, and then it hashes its guid
	// to fins the index, in case of collision it moves forward and also circles around if needed.
	// This could be advantageous as decrypting whole list sequentially gives high overhead.
	private final JSONArray anonymizedIDToGUIDMapping;
	
	public AnonymizedIDEntry(String anonymizedIDString, HashMap<String, Boolean> attributeMap, 
			JSONArray guidSet, JSONArray anonymizedIDToGUIDMapping)
	{
		assert(anonymizedIDString.length() > 0);
		assert(attributeMap != null);
		assert(guidSet != null);
		assert(anonymizedIDToGUIDMapping != null);
		assert(attributeMap.size()>0);
		assert(guidSet.length()>0);
		assert(anonymizedIDToGUIDMapping.length() == guidSet.length());
		
		this.anonymizedIDString = anonymizedIDString;
		this.attributeMap = attributeMap;
		this.guidSet = guidSet;
		this.anonymizedIDToGUIDMapping = anonymizedIDToGUIDMapping;
	}
	
	
	public String getID()
	{
		return anonymizedIDString;
	}
	
	public HashMap<String, Boolean> getAttributeMap()
	{
		return attributeMap;
	}
	
	public JSONArray getGUIDSet()
	{
		return guidSet;
	}
	
	public JSONArray getAnonymizedIDToGUIDMapping()
	{
		return this.anonymizedIDToGUIDMapping;
	}
	
	public String toString()
	{
		String str = "ID: "+anonymizedIDString+" AttrMap: "+attributeMap+" GuidSet: ";
		for(int i=0;i<guidSet.length();i++)
		{
			byte[] guidBytes;
			try 
			{
				guidBytes = (byte[]) guidSet.get(i);
				str=str+Utils.bytArrayToHex(guidBytes)+",";
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
		return str;
	}
}