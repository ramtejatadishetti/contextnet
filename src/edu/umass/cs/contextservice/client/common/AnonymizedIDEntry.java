package edu.umass.cs.contextservice.client.common;

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
	private final byte[] anonymizedIDByteArray;
	private final JSONArray attributeSet;
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
	
	public AnonymizedIDEntry(byte[] anonymizedIDByteArray, JSONArray attributeSet, 
			JSONArray guidSet, JSONArray anonymizedIDToGUIDMapping)
	{
		assert(anonymizedIDByteArray != null);
		assert(attributeSet != null);
		assert(guidSet != null);
		assert(anonymizedIDToGUIDMapping != null);
		assert(attributeSet.length()>0);
		assert(guidSet.length()>0);
		assert(anonymizedIDToGUIDMapping.length() == guidSet.length());
		
		this.anonymizedIDByteArray = anonymizedIDByteArray;
		this.attributeSet = attributeSet;
		this.guidSet = guidSet;
		this.anonymizedIDToGUIDMapping = anonymizedIDToGUIDMapping;
	}
	
	
	public byte[] getID()
	{
		return anonymizedIDByteArray;
	}
	
	public JSONArray getAttributeSet()
	{
		return attributeSet;
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
		String str = "ID: "+Utils.bytArrayToHex(anonymizedIDByteArray)+" AttrSet: "+attributeSet+" GuidSet: ";
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