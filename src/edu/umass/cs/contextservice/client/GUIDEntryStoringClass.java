package edu.umass.cs.contextservice.client;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Stores the GUID entry of a user.
 * @author adipc
 *
 */
public class GUIDEntryStoringClass
{
	public static final String GUID_KEY					= "GUID";
	public static final String PUBLICKEY_KEY			= "PUBLIC_KEY";
	public static final String PRIVATEKEY_KEY			= "PRIVATE_KEY";
	
	private final byte[] guidByteArray;
	private final byte[] publicKeyByteArray;
	private final byte[] privateKeyByteArray;
	
	
	public GUIDEntryStoringClass(byte[] guidByteArray, byte[] publicKeyByteArray, 
			byte[] privateKeyByteArray)
	{
		this.guidByteArray 			= guidByteArray;
		this.publicKeyByteArray 	= publicKeyByteArray;
		this.privateKeyByteArray 	= privateKeyByteArray;
	}
	
	public byte[] getGuidByteArray()
	{
		return guidByteArray;
	}
	
	public byte[] getPublicKeyByteArray()
	{
		return this.publicKeyByteArray;
	}
	
	public byte[] getPrivateKeyByteArray()
	{
		return this.privateKeyByteArray;
	}
	
	/*public JSONObject toJSONObject() throws JSONException
	{
		JSONObject jsonObject = new JSONObject();
		jsonObject.put(GUID_KEY, guidByteArray);
		jsonObject.put(PUBLICKEY_KEY, publicKeyByteArray);
		jsonObject.put(PRIVATEKEY_KEY, privateKeyByteArray);
		
		return jsonObject;
	}
	
	
	public static GUIDEntryStoringClass fromJSONObject(JSONObject jsonObject) throws JSONException
	{
		byte[] guidBytes = (byte[]) jsonObject.get(GUID_KEY);
		byte[] publicKeyByteArray  = (byte[]) jsonObject.get(PUBLICKEY_KEY);
		byte[] privateKeyByteArray = (byte[]) jsonObject.get(PRIVATEKEY_KEY);
		
		return new GUIDEntryStoringClass(guidBytes, publicKeyByteArray, 
				privateKeyByteArray);
	}*/
}