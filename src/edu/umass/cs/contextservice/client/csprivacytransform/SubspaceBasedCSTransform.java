package edu.umass.cs.contextservice.client.csprivacytransform;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.client.common.GUIDEntryStoringClass;
import edu.umass.cs.contextservice.utils.Utils;

public class SubspaceBasedCSTransform implements CSPrivacyTransformInterface
{
	@Override
	public List<CSTransformedUpdatedMessage> transformUpdateForCSPrivacy(String targetGuid, JSONObject attrValuePairs,
			HashMap<String, List<ACLEntry>> aclMap, List<AnonymizedIDEntry> anonymizedIDList) 
	{
		try
		{
			 List<CSTransformedUpdatedMessage> transformedMesgList 	= new LinkedList<CSTransformedUpdatedMessage>();
			
			// a map is computed that contains anonymized IDs whose Gul, guid set, intersects with
			// the ACL of the attributes. The map is from the anonymized IDs to the set of attributes
			// that needs to be updated for that anonymized ID, this set is subset of attrs in attrValuePairs json.
			
			HashMap<byte[], List<AttributeUpdateInfo>> anonymizedIDToAttributesMap 
								= new HashMap<byte[], List<AttributeUpdateInfo>>();
			
			Iterator<String> updatedAttrIter = attrValuePairs.keys();
			while( updatedAttrIter.hasNext() )
			{
				String updAttr = updatedAttrIter.next();
				
				for( int i=0;i<anonymizedIDList.size();i++ )
				{
					AnonymizedIDEntry anonymizedID 
									= (AnonymizedIDEntry)anonymizedIDList.get(i);
					
					JSONArray intersectioPublicKeyMembers 
					= computeTheInsectionOfACLAndAnonymizedIDGuidSet(aclMap.get(updAttr),
							anonymizedID.getGUIDSet() );
					
					if( intersectioPublicKeyMembers.length() > 0 )
					{
						List<AttributeUpdateInfo> attrUpdateList 
								= anonymizedIDToAttributesMap.get(anonymizedID.getID());
						
						if( attrUpdateList == null )
						{
							attrUpdateList = new LinkedList<AttributeUpdateInfo>();
							AttributeUpdateInfo attrUpdObj = new AttributeUpdateInfo(updAttr, intersectioPublicKeyMembers);
							attrUpdateList.add(attrUpdObj);
							anonymizedIDToAttributesMap.put(anonymizedID.getID(), attrUpdateList);		
						}
						else
						{
							AttributeUpdateInfo attrUpdObj = new AttributeUpdateInfo(updAttr, intersectioPublicKeyMembers);
							attrUpdateList.add(attrUpdObj);
						}
					}
				}
			}
			
			
			// now all the anonymized IDs and the attributes that needs to be updated
			// are calculated, 
			// just need to send out updates
			//HashMap<byte[], List<AttributeUpdateInfo>> anonymizedIDToAttributesMap 
			// = new HashMap<byte[], List<AttributeUpdateInfo>>();
			Iterator<byte[]> anonymizedIter = anonymizedIDToAttributesMap.keySet().iterator();
			
			while( anonymizedIter.hasNext() )
			{
				byte[] anonymizedID 	= anonymizedIter.next();
				List<AttributeUpdateInfo> updateAttrList 
										= anonymizedIDToAttributesMap.get(anonymizedID);
				
				getTransformedMessageList(anonymizedID, Utils.hexStringToByteArray(targetGuid), 
						updateAttrList, attrValuePairs, transformedMesgList);
			}
			
		return transformedMesgList;	
		}
		catch(Exception | Error ex)
		{
			ex.printStackTrace();
		}
		
		return null;
	}

	@Override
	public List<String> unTransformSearchReply(List<CSTransformedUpdatedMessage> csTransformedList) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	/**
	 * First argument is public key, second is GUID. need to convert that to match.
	 * @param publicKeyACLMembers
	 * @return null if no intersection
	 * @throws JSONException 
	 */
	private JSONArray computeTheInsectionOfACLAndAnonymizedIDGuidSet
		(List<ACLEntry> aclEntriesForAttr, JSONArray guidSetOfAnonymizedID) throws JSONException
	{
		JSONArray intersection = new JSONArray();
		
		HashMap<byte[], Boolean> guidMapForAnonymizedID = new HashMap<byte[], Boolean>();
		
		for( int i=0; i < guidSetOfAnonymizedID.length(); i++)
		{
			byte[] guid = (byte[]) guidSetOfAnonymizedID.get(i);
			guidMapForAnonymizedID.put(guid, true);
		}
		
		for(int i=0; i<aclEntriesForAttr.size(); i++)
		{
			//byte[] publicKeyByteArray = (byte[]) aclEntriesForAttr.get(i).getPublicKeyACLMember();
			byte[] guid = aclEntriesForAttr.get(i).getACLMemberGUID();
			
			// intersection
			if( guidMapForAnonymizedID.containsKey(guid) )
			{
				intersection.put(guid);
			}
		}
		return intersection;
	}
	
	
	private List<CSTransformedUpdatedMessage> getTransformedMessageList(byte[] anonymizedID, byte[] realGUID, 
			List<AttributeUpdateInfo> updateAttrList, JSONObject csAttrValuePairs, List<CSTransformedUpdatedMessage> transformMessageList)
	{
//		String IDString = Utils.bytArrayToHex(anonymizedID);
//		ContextServiceLogger.getLogger().fine("ContextClient sendUpdate enter "+GUID+" json "+
//				gnsAttrValuePairs);
		try
		{		
			JSONObject csAttrValuePairsSec = new JSONObject();
			JSONObject encryptedRealIDPair = new JSONObject();
			
			for( int i=0; i<updateAttrList.size(); i++ )
			{
				AttributeUpdateInfo attrUpdInfo = updateAttrList.get(i);
				
				// getting the updated value from the user supplied JSON.
				String value = csAttrValuePairs.getString(attrUpdInfo.getAttrName());
				addSecureValueInJSON( realGUID, csAttrValuePairsSec, encryptedRealIDPair, 
						attrUpdInfo, value );
				
				CSTransformedUpdatedMessage transforMessage = new CSTransformedUpdatedMessage
						(anonymizedID, csAttrValuePairsSec, encryptedRealIDPair);
				transformMessageList.add(transforMessage);
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		catch(Exception | Error ex)
		{
			ex.printStackTrace();
		}
		return transformMessageList;
		// no waiting in update	
	}
	
	
	/**
	 * This function add attr:value pair in JSONObject, 
	 * This function also encrypts anonymized ID by encrypting with 
	 * ACL members public key.
	 * This may change, if in future we change it to a multi recipient secret 
	 * sharing.
	 * @param csAttrValuePairsSec
	 * @param attrUpdInfo
	 * @param value
	 * @throws JSONException 
	 */
	private void addSecureValueInJSON(byte[] realGUID, JSONObject csAttrValuePairs, 
			JSONObject encryptedRealIDPair, AttributeUpdateInfo attrUpdInfo, String value) throws JSONException
	{
		JSONArray encryptedRealIDArray = new JSONArray();
		JSONArray publicKeyArray = attrUpdInfo.publicKeyArray;
		
		for( int i=0; i<publicKeyArray.length(); i++ )
		{
			// catching here so that one bad key doesn't let everything else to fail.
			try 
			{
				byte[] publicKey = (byte[]) publicKeyArray.get(i);
				byte[] encryptedRealID = 
						 Utils.doPublicKeyEncryption(publicKey, realGUID);
				
				encryptedRealIDArray.put(encryptedRealID);
				
			} catch (NoSuchAlgorithmException e) 
			{
				e.printStackTrace();
			} catch (InvalidKeySpecException e) 
			{
				e.printStackTrace();
			} catch (JSONException e) 
			{
				e.printStackTrace();
			} catch (InvalidKeyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchPaddingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalBlockSizeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (BadPaddingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		//ValueClassInPrivacy valueObj = new ValueClassInPrivacy(value, encryptedRealIDArray);
		// add the above value in the JSON
		csAttrValuePairs.put(attrUpdInfo.getAttrName(), value);
		
		encryptedRealIDPair.put(attrUpdInfo.getAttrName(), encryptedRealIDArray);
	}
	
	
	/**
	 * Decrypts the anonymized IDs .
	 * The result is returned in replyArray.
	 * @param myGUIDInfo
	 * @param encryptedRealIDArray
	 * @param replyArray
	 * @return
	 */
	private void decryptAnonymizedIDs(GUIDEntryStoringClass myGUIDInfo, 
			JSONArray encryptedRealIDArray, JSONArray replyArray)
	{
		for( int i=0; i<encryptedRealIDArray.length(); i++ )
		{
			try
			{
				JSONObject currJSON = encryptedRealIDArray.getJSONObject(i);
				Iterator<String> anonymizedIDIter = currJSON.keys();
				
				while( anonymizedIDIter.hasNext() )
				{
					String anonymizedIDHex = anonymizedIDIter.next();
					// each element is byte[], 
					// convert it to String, then to JSONArray.
					// each element of JSONArray is encrypted RealID.
					// each element needs to be decrypted with private key.
					
					byte[] jsonArrayByteArray = (byte[]) currJSON.get(anonymizedIDHex);
					// this operation can be inefficient
					String jsonArrayString = new String(jsonArrayByteArray);
					
					JSONArray jsonArray = new JSONArray(jsonArrayString);
					
					byte[] resultGUIDBytes = decryptTheRealID( myGUIDInfo, jsonArray );
					
					replyArray.put(new String(resultGUIDBytes));
				}
			} catch ( JSONException e )
			{
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * this function decrypts and returns the real ID from json Array of encrypted values.
	 * @param myGUIDInfo
	 * @param encryptedRealJsonArray
	 * @return
	 * @throws JSONException 
	 */
	private byte[] decryptTheRealID( GUIDEntryStoringClass myGUIDInfo, 
			JSONArray encryptedRealJsonArray ) throws JSONException
	{
		byte[] privateKey = myGUIDInfo.getPrivateKeyByteArray();
		byte[] plainText = null;
		boolean found = false;
		for( int i=0; i<encryptedRealJsonArray.length(); i++ )
		{
			byte[] encryptedElement = (byte[]) encryptedRealJsonArray.get(i);
			
			try
			{
				plainText = Utils.doPrivateKeyDecryption(privateKey, encryptedElement);
				// non exception, just break;
				break;
			}
			catch(javax.crypto.BadPaddingException wrongKeyException)
			{
				// just catching this one, as this one results when wrong key is used 
				// to decrypt.
			} catch (InvalidKeyException e) 
			{
				e.printStackTrace();
			} catch (NoSuchAlgorithmException e) 
			{
				e.printStackTrace();
			} catch (InvalidKeySpecException e) 
			{
				e.printStackTrace();
			} catch (NoSuchPaddingException e) 
			{
				e.printStackTrace();
			} catch (IllegalBlockSizeException e) 
			{
				e.printStackTrace();
			}
		}
		assert(plainText != null);
		return plainText;
	}
	
	
	/**
	 * This class stores attribute info on an update 
	 * for each anonymized ID.
	 * 
	 * @author adipc
	 */
	private class AttributeUpdateInfo
	{
		private final String attrName;
		
		// this is the array of public key members that are common in this 
		// attribute's ACL and the corresponding anonymized IDs guid set, Gul.
		// this array is used to encrypt the real ID of the user. 
		private final JSONArray publicKeyArray;
		
		public AttributeUpdateInfo(String attrName, JSONArray publicKeyArray)
		{
			this.attrName = attrName;
			this.publicKeyArray = publicKeyArray;
		}
		
		public String getAttrName()
		{
			return this.attrName;
		}
		
		public JSONArray getPublicKeyArray()
		{
			return this.publicKeyArray;
		}
	}
}