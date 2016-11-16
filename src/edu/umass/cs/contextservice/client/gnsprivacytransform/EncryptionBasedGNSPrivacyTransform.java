package edu.umass.cs.contextservice.client.gnsprivacytransform;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gnsclient.client.util.GuidEntry;

public class EncryptionBasedGNSPrivacyTransform implements GNSPrivacyTransformInterface
{
	private final KeyGenerator keyGen;
	
	public EncryptionBasedGNSPrivacyTransform() throws NoSuchAlgorithmException
	{
		keyGen = KeyGenerator.getInstance(ContextServiceConfig.SymmetricEncAlgorithm);
	}
	
	@Override
	public GNSTransformedMessage transformUpdateForGNSPrivacy(JSONObject attrValuePair,
			HashMap<String, List<ACLEntry>> aclMap)
	{
		//Key symKey = KeyGenerator.getInstance(algorithm).generateKey();
		JSONObject encryptedAttrValuePairs = new JSONObject();
		// JSON iter warning
		@SuppressWarnings("unchecked")
		Iterator<String> attrIter = attrValuePair.keys();
		while(attrIter.hasNext())
		{	
			try {
				String attrName = attrIter.next();
				String value = attrValuePair.getString(attrName);
				EncryptedValueJSON encryptValue;
				encryptValue = getEncryptedValueJSONForAttr( value, 
						aclMap.get(attrName) );
				encryptedAttrValuePairs.put(attrName, encryptValue.toJSONObject());
			} catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException
					| IllegalBlockSizeException | BadPaddingException | InvalidKeySpecException | JSONException e) {
				e.printStackTrace();
			}
		}
		GNSTransformedMessage gnsTransMesg = new GNSTransformedMessage(encryptedAttrValuePairs);
		
		return gnsTransMesg;
	}
	
	@Override
	public JSONObject unTransformGetReply(GNSTransformedMessage gnsTransformedMessage, GuidEntry myGuidEntry) 
	{
		JSONObject palinTextAttrValuePairs = new JSONObject();
		JSONObject encryptedAttrValPairs = gnsTransformedMessage.getEncryptedAttrValuePair();
		
		// JSON iter warning
		@SuppressWarnings("unchecked")
		Iterator<String> attrIter = encryptedAttrValPairs.keys();
		
		while(attrIter.hasNext())
		{
			try
			{
				String attrName = attrIter.next();
				EncryptedValueJSON encryptValueJSON = EncryptedValueJSON.fromJSONObject
						(encryptedAttrValPairs.getJSONObject(attrName));
				
				String plainVal = getPlainValueString(encryptValueJSON, myGuidEntry);
				if(plainVal != null)
				{
					palinTextAttrValuePairs.put(attrName, plainVal);
				}
			} catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}	
		return palinTextAttrValuePairs;
	}
	
	/**
	 * Returns the encrypted value JSON for an attribtue.
	 * AttrName encrypted value JSON is stored in GNS.
	 * @return
	 * @throws BadPaddingException 
	 * @throws IllegalBlockSizeException 
	 * @throws NoSuchPaddingException 
	 * @throws NoSuchAlgorithmException 
	 * @throws InvalidKeyException 
	 * @throws InvalidKeySpecException 
	 * @throws JSONException 
	 */
	private EncryptedValueJSON getEncryptedValueJSONForAttr( String plainValue, 
			List<ACLEntry> aclListForAttr ) throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException, InvalidKeySpecException, JSONException
	{
		SecretKey symKey = keyGen.generateKey();
		byte[] encryptedValue = Utils.doSymmetricEncryption
				(symKey.getEncoded(), plainValue.getBytes());
		JSONObject decryptValueInfo  = new JSONObject();
		
		for(int i=0; i<aclListForAttr.size();i++)
		{
			ACLEntry aclEntry = aclListForAttr.get(i);
			byte[] encryptedSymKey = Utils.doPublicKeyEncryption
					(aclEntry.getPublicKeyACLMember(), symKey.getEncoded());
			
			String guidString = Utils.byteArrayToHex(aclEntry.getACLMemberGUID());
			
			// NIP cannot send byte[]
			decryptValueInfo.put(guidString, Utils.byteArrayToHex(encryptedSymKey));			
		}
		
		EncryptedValueJSON encryptJSON = new EncryptedValueJSON(
				Utils.byteArrayToHex(encryptedValue), decryptValueInfo);
		
		return encryptJSON;
	}
	
	
	private String getPlainValueString(EncryptedValueJSON encryptValJSON, 
			GuidEntry myGuidEntry) 
		throws JSONException, InvalidKeyException, NoSuchAlgorithmException, InvalidKeySpecException, NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException
	{
		String encryptedVal = encryptValJSON.getEncytpedValue();
		JSONObject decryptValueInfo = encryptValJSON.getDecryptValueInfo();
		String myGUIDString = myGuidEntry.getGuid();
	
		if(decryptValueInfo.has(myGUIDString))
		{
			String encryptedSymKeyHexString = decryptValueInfo.getString(myGUIDString);
			byte[] encryptedSymKeyBytes = Utils.hexStringToByteArray(encryptedSymKeyHexString);
			byte[] symKeyBytes = Utils.doPrivateKeyDecryption(myGuidEntry.getPrivateKey().getEncoded(), 
					encryptedSymKeyBytes);
			
			byte[] valueBytes = Utils.doSymmetricDecryption(symKeyBytes, 
					Utils.hexStringToByteArray(encryptedVal));
			
			
			String plainValueString = new String(valueBytes);
			return plainValueString;
		}
		return null;
	}
}