package edu.umass.cs.contextservice.client.gnsprivacytransform;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class denotes the JSONObject representation 
 * of the encrypted value. It contains the encrypted value 
 * and the associated ACL info to decrypt it.
 * @author adipc
 */
public class EncryptedValueJSON 
{
	private enum Keys {ENCRYPTED_VALUE, DECRYPT_VALUE_INFO};
	
	// byte[] array with NIO not working, so changing it to hex format String
	// byte arrays should be used instead if strings,
	// as string just double the space, 
	// and encrypted values are already very large in size.
	private final String encytpedValue; 
	
	// the key of this JSONObject is guids in ACL, like G1 ,and the value is
	// and  enc(G1+, Ks), where G1+ is the public key of G1
	// and Ks is the symmetric key with which the value is encrypted.
	// having GUID G1 as the key helps in directly obtaining the decrypting
	// info rather than checking all members of ACL, decrypting and failing until
	// the owners own GUID is found in the ACL.
	// TODO: GNS can also just return the decrypting info of the GUID that is
	// doing the lookup, instead returning all members of ACL. but that requires
	// more changes in GNS for privacy stuff.
	private final JSONObject decryptValueInfo;
	
	
	public EncryptedValueJSON(String encytpedValue, JSONObject decryptValueInfo)
	{
		this.encytpedValue = encytpedValue;
		this.decryptValueInfo = decryptValueInfo;
	}
	
	public String getEncytpedValue()
	{
		return encytpedValue;
	}
	
	public JSONObject  getDecryptValueInfo()
	{
		return decryptValueInfo;
	}
	
	public JSONObject toJSONObject() throws JSONException
	{
		JSONObject jsonObject = new JSONObject();
		jsonObject.put(Keys.ENCRYPTED_VALUE.toString(), encytpedValue);
		jsonObject.put(Keys.DECRYPT_VALUE_INFO.toString(), decryptValueInfo);
		return jsonObject;
	}
	
	public static EncryptedValueJSON fromJSONObject(JSONObject jsonObject) throws JSONException
	{
		String encryptedValue = jsonObject.getString(Keys.ENCRYPTED_VALUE.toString());
		JSONObject decryptValueInfo = jsonObject.getJSONObject(Keys.DECRYPT_VALUE_INFO.toString());
		
		EncryptedValueJSON encryptJSON = new EncryptedValueJSON(encryptedValue, 
				decryptValueInfo);
		
		return encryptJSON;
	}
}