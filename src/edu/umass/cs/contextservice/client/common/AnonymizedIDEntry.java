package edu.umass.cs.contextservice.client.common;

import java.util.HashMap;
import java.util.Iterator;

import javax.crypto.SecretKey;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.utils.Utils;

/**
 * 
 * The class represents anonymized ID and its 
 * associated attributes and guids.
 * @author adipc
 */
public class AnonymizedIDEntry 
{
	private final String anonymizedIDString;
	private final HashMap<String, Boolean> attributeMap;
	
	// GUID is a byte[] not  a string.
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
	
	private final JSONArray attrSetAsArray;
	
	/**
	 * secret key corresponding to this anonymized ID.
	 * This secret key is shared with guidSet.
	 */
	private final SecretKey anonymizedIDSecreyKey;
	
	// key in this map is the GUID from guidSet in String format.
	// value is the encryption of secret key with the public key of GUID and converting into 
	// String form.
	private final HashMap<String, String> secretKeySharingMap;
	
	public AnonymizedIDEntry(String anonymizedIDString, HashMap<String, Boolean> attributeMap, 
			JSONArray guidSet, JSONArray anonymizedIDToGUIDMapping, 
			SecretKey anonymizedIDSecreyKey, HashMap<String, String> secretKeySharingMap )
	{
		assert(anonymizedIDString.length() > 0);
		assert(attributeMap != null);
		assert(guidSet != null);
		assert(anonymizedIDToGUIDMapping != null);
		assert(attributeMap.size() > 0);
		assert(guidSet.length() > 0);
		
		if( anonymizedIDSecreyKey == null)
		{
			assert( anonymizedIDToGUIDMapping.length() == guidSet.length() );
		}
		else 
		{
			assert( anonymizedIDToGUIDMapping.length() == 1 );
		}	  
		
		this.anonymizedIDString = anonymizedIDString;
		this.attributeMap = attributeMap;
		this.guidSet = guidSet;
		this.anonymizedIDToGUIDMapping = anonymizedIDToGUIDMapping;
		this.anonymizedIDSecreyKey = anonymizedIDSecreyKey;
		this.secretKeySharingMap = secretKeySharingMap;
		
		attrSetAsArray = new JSONArray();
		
		
		Iterator<String> attrIter = attributeMap.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			attrSetAsArray.put(attrName);
		}
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
	
	public SecretKey getAnonymizedIDSecretKey()
	{
		return this.anonymizedIDSecreyKey;
	}
	
	public JSONArray getAttrSet()
	{
		return this.attrSetAsArray;
	}
	
	public HashMap<String, String> getSecretKeySharingMap()
	{
		return this.secretKeySharingMap;
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
				str=str+Utils.byteArrayToHex(guidBytes)+",";
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
		
		str=str +" anonymizedIDToGUIDMapping "+anonymizedIDToGUIDMapping;
		
		return str;
	}
}