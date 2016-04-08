package edu.umass.cs.contextservice.client;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Class that is used to represent ACL and also contains a 
 * toJSONObject methods to convert class's object into JSONObject
 * @author adipc
 */
public class ACLStoringClass
{
	// JSON keys
	public static final String attrNameKey						= "attrNameKey";
	public static final String publicKeyACLMembersKey			= "publicKeyACLMembersKey";
	
	private final String attrName;
	
	// stores byte arrays, not the strings, strings double the amount of space.
	// stores public keys, not GUIDs, as they can be constructed any time.
	// public key is needed for encryption as well.
	private final JSONArray publicKeyACLMembers;
	
	public ACLStoringClass(String attrName, JSONArray publicKeyACLMembers)
	{
		this.attrName = attrName;
		this.publicKeyACLMembers = publicKeyACLMembers;
	}
	
	public String getAttrName()
	{
		return this.attrName;
	}
	
	public JSONArray getPublicKeyACLMembers()
	{
		return publicKeyACLMembers;
	}
	
	/*public JSONObject toJSONObject() throws JSONException
	{
		JSONObject jsonObject = new JSONObject();
		jsonObject.put(attrNameKey, attrName);
		jsonObject.put(publicKeyACLMembersKey, publicKeyACLMembers);
		return jsonObject;
	}
	
	public static ACLStoringClass fromJSONObject(JSONObject jsonObject) throws JSONException
	{
		return new ACLStoringClass
				( jsonObject.getString(attrNameKey), jsonObject.getJSONArray(publicKeyACLMembersKey) );
	}*/
}