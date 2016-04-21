package edu.umass.cs.contextservice.client.csprivacytransform;

import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.client.anonymizedID.SubspaceBasedAnonymizedIDCreator;
import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.messages.dataformat.AttrValueRepresentationJSON;
import edu.umass.cs.contextservice.messages.dataformat.SearchReplyGUIDRepresentationJSON;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;

public class SubspaceBasedCSTransform implements CSPrivacyTransformInterface
{
	private final ExecutorService exectutorService;
	
	public SubspaceBasedCSTransform(ExecutorService exectutorService)
	{
		this.exectutorService = exectutorService;
	}
	
	@Override
	public List<CSUpdateTransformedMessage> transformUpdateForCSPrivacy(String targetGuid, 
			HashMap<String, AttrValueRepresentationJSON> attrValueMap , 
			HashMap<String, List<ACLEntry>> aclMap, 
			List<AnonymizedIDEntry> anonymizedIDList)
	{
		try
		{
			 List<CSUpdateTransformedMessage> transformedMesgList 
			 						= new LinkedList<CSUpdateTransformedMessage>();
			
			// a map is computed that contains anonymized IDs whose Gul, guid set, intersects with
			// the ACL of the attributes. The map is from the anonymized IDs to the set of attributes
			// that needs to be updated for that anonymized ID, this set is subset of attrs 
			// in attrValueMap.
			
			HashMap<String, List<AttributeUpdateInfo>> anonymizedIDToAttributesMap 
				= computeAnonymizedIDToAttributesMap(attrValueMap, aclMap, anonymizedIDList);
			
			// now all the anonymized IDs and the attributes that needs to be updated
			// are calculated, 
			// just need to send out updates
			//HashMap<byte[], List<AttributeUpdateInfo>> anonymizedIDToAttributesMap 
			// = new HashMap<byte[], List<AttributeUpdateInfo>>();
			Iterator<String> anonymizedIter = anonymizedIDToAttributesMap.keySet().iterator();
			
			while( anonymizedIter.hasNext() )
			{
				String anonymizedIDString 	= anonymizedIter.next();
				byte[] anonymizedIDBytes    = Utils.hexStringToByteArray(anonymizedIDString);
				List<AttributeUpdateInfo> updateAttrList 
										= anonymizedIDToAttributesMap.get(anonymizedIDString);
				
				HashMap<String, AttrValueRepresentationJSON> currAttrValueMap = 
						getTransformedAnonymizedIDUpdateAttrValueMap(anonymizedIDBytes, Utils.hexStringToByteArray(targetGuid), 
						updateAttrList, attrValueMap, transformedMesgList);
				
				CSUpdateTransformedMessage transforMessage = new CSUpdateTransformedMessage
						(anonymizedIDBytes, currAttrValueMap);
				
				transformedMesgList.add(transforMessage);
			}
			
			if( ContextServiceConfig.DEBUG_MODE )
			{
				int totalEncryptions = 
						calculateTotalEncryptionsOnAnUpdate( anonymizedIDToAttributesMap );
				System.out.println("targetGuid "+targetGuid+" number attrs updated "+attrValueMap.size()
				+ " number of anonymized IDs updated "+anonymizedIDToAttributesMap.size()+
				" total encryptions "+totalEncryptions);
			}
			
			// just returning 1 anonymized ID for testing update secure latency
			List<CSUpdateTransformedMessage> testMesgList 	
				= new LinkedList<CSUpdateTransformedMessage>();
			if( transformedMesgList.size() > 0 )
			{
				testMesgList.add(transformedMesgList.get(0));
			}
			//return testMesgList;
			return transformedMesgList;	
		}
		catch(Exception | Error ex)
		{
			ex.printStackTrace();
		}
		return null;
	}

	@Override
	public void unTransformSearchReply(GuidEntry myGuid, 
			List<CSSearchReplyTransformedMessage> csTransformedList
			, JSONArray replyArray)
	{
		
		ParallelSearchReplyDecryption parallelSearchDecryption =
				new ParallelSearchReplyDecryption(myGuid , csTransformedList
						, replyArray, exectutorService);
		
		
		parallelSearchDecryption.doDecryption();
		
		if(ContextServiceConfig.DEBUG_MODE)
		{
			System.out.println("Total decryptions "+parallelSearchDecryption.getTotalDecryptionsOverall());
		}
		
//		for(int i=0; i<csTransformedList.size();i++)
//		{
//			CSSearchReplyTransformedMessage csSearchRepMessage 
//							= csTransformedList.get(i);
//			
//			byte[] realGuidBytes 
//					= decryptRealIDFromSearchRep(myGuid, csSearchRepMessage.getSearchGUIDObj());
//			
//			// just adding the  ID
//			if(realGuidBytes == null)
//			{
//				ContextServiceLogger.getLogger().info("Unable to map this ID "+
//						csSearchRepMessage.getSearchGUIDObj().getID());
//			}
//			else
//			{
//				replyArray.put(Utils.bytArrayToHex(realGuidBytes));
//			}
//		}
	}
	
	/**
	 * Computes a map that contains anonymized IDs whose Gul, guid set, intersects with
	 * the ACL of the attributes. The map is from the anonymized IDs to the set of attributes
	 * that needs to be updated for that anonymized ID, this set is subset of attrs 
	 * in attrValueMap.
	 * @param attrValueMap
	 * @param anonymizedIDList
	 * @return
	 * @throws JSONException 
	 */
	private HashMap<String, List<AttributeUpdateInfo>> 
		computeAnonymizedIDToAttributesMap(HashMap<String, AttrValueRepresentationJSON> attrValueMap
				, HashMap<String, List<ACLEntry>> aclMap 
				, List<AnonymizedIDEntry> anonymizedIDList) throws JSONException
	{
		HashMap<String, List<AttributeUpdateInfo>> anonymizedIDToAttributesMap 
		= new HashMap<String, List<AttributeUpdateInfo>>();

		Iterator<String> attrIter = attrValueMap.keySet().iterator();

		while( attrIter.hasNext() )
		{
			String updAttr = attrIter.next();

			for( int i=0;i<anonymizedIDList.size();i++ )
			{
				AnonymizedIDEntry anonymizedID 
					= anonymizedIDList.get(i);

				//byte[] elements
				List<ACLEntry> intersectioACLEntries 
					= computeTheInsectionOfACLAndAnonymizedIDGuidSet(aclMap.get(updAttr),
						anonymizedID.getGUIDSet() );

				if( intersectioACLEntries.size() > 0 )
				{
					String anonymizedIDString = Utils.bytArrayToHex(anonymizedID.getID());
					List<AttributeUpdateInfo> attrUpdateList 
						= anonymizedIDToAttributesMap.get(anonymizedIDString);

					if( attrUpdateList == null )
					{
						attrUpdateList = new LinkedList<AttributeUpdateInfo>();
						AttributeUpdateInfo attrUpdObj 
							= new AttributeUpdateInfo(updAttr, intersectioACLEntries);
						attrUpdateList.add(attrUpdObj);
						anonymizedIDToAttributesMap.put(anonymizedIDString, attrUpdateList);		
					}
					else
					{
						AttributeUpdateInfo attrUpdObj = new AttributeUpdateInfo(updAttr, 
								intersectioACLEntries);
						attrUpdateList.add(attrUpdObj);
					}
				}
			}
		}
		
		return anonymizedIDToAttributesMap;
	}
	
	
	/**
	 * computes the interection of ACL of an attribute with the guid set of an 
	 * anonymized ID. 
	 * Returns List<ACLEntry> that intersect, these are used in encrypting realdIDs
	 * @param aclEntriesForAttr
	 * @param guidSetOfAnonymizedID
	 * @return
	 * @throws JSONException
	 */
	private List<ACLEntry> computeTheInsectionOfACLAndAnonymizedIDGuidSet
		(List<ACLEntry> aclEntriesForAttr, JSONArray guidSetOfAnonymizedID) throws JSONException
	{
		
		List<ACLEntry> intersectionACLEntries = new LinkedList<ACLEntry>();
		
		HashMap<String, Boolean> guidMapForAnonymizedID = new HashMap<String, Boolean>();
		
		for( int i=0; i < guidSetOfAnonymizedID.length(); i++)
		{
			byte[] guid = (byte[]) guidSetOfAnonymizedID.get(i);
			guidMapForAnonymizedID.put(Utils.bytArrayToHex(guid), true);
		}
		
		for(int i=0; i<aclEntriesForAttr.size(); i++)
		{
			ACLEntry currACLEntry = aclEntriesForAttr.get(i);
			//byte[] publicKeyByteArray = (byte[]) aclEntriesForAttr.get(i).getPublicKeyACLMember();
			byte[] guid = currACLEntry.getACLMemberGUID();
			
			// intersection
			if( guidMapForAnonymizedID.containsKey( Utils.bytArrayToHex(guid) ) )
			{
				intersectionACLEntries.add(currACLEntry);
			}
		}
		return intersectionACLEntries;
	}
	
	
	/**
	 * This function computes the attrValue pair map for a anonymizedID.
	 * It computes all the attributs that needs to be updateed for the 
	 * anonymized ID and also computes the realIDMapping Info for each attibute.
	 * 
	 * @param anonymizedID
	 * @param realGUID
	 * @param updateAttrList
	 * @param attrValueMap
	 * @param transformMessageList
	 * @return
	 */
	private HashMap<String, AttrValueRepresentationJSON> getTransformedAnonymizedIDUpdateAttrValueMap(
			byte[] anonymizedID, byte[] realGUID, 
			List<AttributeUpdateInfo> updateAttrList, 
			HashMap<String, AttrValueRepresentationJSON> attrValueMap, 
			List<CSUpdateTransformedMessage> transformMessageList)
	{
		// key is attrName
		HashMap<String, AttrValueRepresentationJSON> anonymizedIDSpecificUpdateAttrs
								= new HashMap<String, AttrValueRepresentationJSON>();
		try
		{
			for( int i=0; i<updateAttrList.size(); i++ )
			{
				AttributeUpdateInfo attrUpdInfo = updateAttrList.get(i);
				String currAttrName = attrUpdInfo.getAttrName();
				
				AttrValueRepresentationJSON attrValRep = attrValueMap.get(currAttrName);
				String value = attrValRep.getActualAttrValue();
				
				// create a new AttrValueRepresentationJSON, 
				// because multiple anonymized IDs might update same attribute value pair
				// so using the input AttrValueRepresentationJSON will casuse contention and serious bug
				AttrValueRepresentationJSON currValRep =
						addRealIDMappingInfo( realGUID, value, attrUpdInfo);
				
				anonymizedIDSpecificUpdateAttrs.put(currAttrName, currValRep);		
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		catch(Exception | Error ex)
		{
			ex.printStackTrace();
		}
		return anonymizedIDSpecificUpdateAttrs;
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
	private AttrValueRepresentationJSON addRealIDMappingInfo(byte[] realGUID, 
			String value, 
			AttributeUpdateInfo attrUpdInfo) throws JSONException
	{
		JSONArray realIDMappingInfo = new JSONArray();
		List<ACLEntry> intersectingACLEntries = attrUpdInfo.getIntersectingACLEntries();
		
		for( int i=0; i<intersectingACLEntries.size(); i++ )
		{
			// catching here so that one bad key doesn't let everything else to fail.
			try 
			{
				ACLEntry currACL = intersectingACLEntries.get(i);
				byte[] publicKey = currACL.getPublicKeyACLMember();
				byte[] encryptedRealID = 
						 Utils.doPublicKeyEncryption(publicKey, realGUID);
				
				realIDMappingInfo.put(Utils.bytArrayToHex(encryptedRealID));
				
			} catch (NoSuchAlgorithmException e) 
			{
				e.printStackTrace();
			} catch (InvalidKeySpecException e) 
			{
				e.printStackTrace();
			} catch (InvalidKeyException e) {
				e.printStackTrace();
			} catch (NoSuchPaddingException e) {
				e.printStackTrace();
			} catch (IllegalBlockSizeException e) {
				e.printStackTrace();
			} catch (BadPaddingException e) {
				e.printStackTrace();
			}
		}
		
		AttrValueRepresentationJSON currAttrValRep 
							= new AttrValueRepresentationJSON(value, realIDMappingInfo);
		return currAttrValRep;
	}
	
	/**
	 * 
	 * This function calculates total encryptions on 
	 * an update, just for debugging purposes.
	 * @return
	 */
	private int calculateTotalEncryptionsOnAnUpdate
			( HashMap<String, List<AttributeUpdateInfo>> anonymizedIDToAttributesMap )
	{
		int totalEncryptions = 0;
		Iterator<String> anonymizedIDIter 
						= anonymizedIDToAttributesMap.keySet().iterator();
		
		while( anonymizedIDIter.hasNext() )
		{
			String anonymizedID = anonymizedIDIter.next();
			
			List<AttributeUpdateInfo> attrUpdateInfoList 
										= anonymizedIDToAttributesMap.get(anonymizedID);
			
			for(int i=0; i < attrUpdateInfoList.size(); i++)
			{
				AttributeUpdateInfo attrUpdInfo = attrUpdateInfoList.get(i);
				totalEncryptions = totalEncryptions + attrUpdInfo.getIntersectingACLEntries().size();
			}
		}
		
		return totalEncryptions;
	}
	
	/**
	 * Decrypts the real ID from search reply using realID mapping info.
	 * Returns null if it cannot be decrypted.
	 * @param myGUIDInfo
	 * @param encryptedRealJsonArray
	 * @return
	 * @throws JSONException 
	 */
//	private byte[] decryptRealIDFromSearchRep( GuidEntry myGUIDInfo, 
//			SearchReplyGUIDRepresentationJSON seachReply ) 
//	{
//		byte[] privateKey = myGUIDInfo.getPrivateKey().getEncoded();
//		byte[] plainText = null;
//		boolean found = false;
//		JSONArray realIDMappingInfo = seachReply.getRealIDMappingInfo();
//		if(realIDMappingInfo != null)
//		{
//			ContextServiceLogger.getLogger().fine("realIDMappingInfo JSONArray "
//					+ realIDMappingInfo.length() );
//			
//			for( int i=0; i<realIDMappingInfo.length(); i++ )
//			{	
//				try
//				{
//					byte[] encryptedElement = (byte[]) (Utils.hexStringToByteArray(
//							realIDMappingInfo.getString(i)));
//					
//					plainText = Utils.doPrivateKeyDecryption(privateKey, encryptedElement);
//					// non exception, just break;
//					found = true;
//					break;
//				}
//				catch(javax.crypto.BadPaddingException wrongKeyException)
//				{
//					// just catching this one, as this one results when wrong key is used 
//					// to decrypt.
//				} catch ( InvalidKeyException | NoSuchAlgorithmException
//						| InvalidKeySpecException | NoSuchPaddingException
//						| IllegalBlockSizeException | JSONException
//						e )
//				{
//					e.printStackTrace();
//				}
//			}
//		}
//		
//		if(plainText != null)
//		{
//			ContextServiceLogger.getLogger().fine("Anonymized ID "+seachReply.getID()
//									+ "realID "+Utils.bytArrayToHex(plainText) );
//		}
//		
//		return plainText;
//	}
	
	
	/**
	 * This class stores attribute info on an update 
	 * for each anonymized ID.
	 * 
	 * @author adipc
	 */
	private class AttributeUpdateInfo
	{
		private final String attrName;
		
		// this is the list of ACLEntries that are common in this 
		// attribute's ACL and the corresponding anonymized IDs guid set, Gul.
		// this array is used to encrypt the real ID of the user. 
		private final List<ACLEntry> intersectingACLEntries;
		
		public AttributeUpdateInfo(String attrName, List<ACLEntry> intersectingACLEntries)
		{
			this.attrName = attrName;
			this.intersectingACLEntries = intersectingACLEntries;
		}
		
		public String getAttrName()
		{
			return this.attrName;
		}
		
		public List<ACLEntry> getIntersectingACLEntries()
		{
			return this.intersectingACLEntries;
		}
	}
	
	// test this Class implementation
	public static void main(String[] args) throws NoSuchAlgorithmException
	{
		// testing based on the example in the draft.
		// more testing of each method in secure interface.
		// test with the example in the draft.
		//GUIDEntryStoringClass myGUIDInfo, JSONArray ACLArray
		//String guid = GuidUtils.createGuidFromPublicKey(keyPair.getPublic().getEncoded());
		KeyPairGenerator kpg;
		kpg = KeyPairGenerator.getInstance("RSA");
		KeyPair kp0 = kpg.genKeyPair();
		PublicKey publicKey0 = kp0.getPublic();
		PrivateKey privateKey0 = kp0.getPrivate();
		byte[] publicKeyByteArray0 = publicKey0.getEncoded();
		byte[] privateKeyByteArray0 = privateKey0.getEncoded();
		String guid0 = GuidUtils.createGuidFromPublicKey(publicKeyByteArray0);
		
		Vector<GuidEntry> guidsVector = new Vector<GuidEntry>();
		GuidEntry myGUID = new GuidEntry("Guid0", guid0, publicKey0, privateKey0);

		
		guidsVector.add(myGUID);
		
		// draft example has 7 guids
		for(int i=1; i <= 7; i++)
		{
			KeyPair kp = kpg.genKeyPair();
			PublicKey publicKey = kp.getPublic();
			PrivateKey privateKey = kp.getPrivate();
			byte[] publicKeyByteArray = publicKey.getEncoded();
			byte[] privateKeyByteArray = privateKey.getEncoded();
			
			String guid = GuidUtils.createGuidFromPublicKey(publicKeyByteArray);
			
			GuidEntry currGUID = new GuidEntry("Guid"+i, guid, 
					publicKey, privateKey);
			
			guidsVector.add(currGUID);
		}
		
		HashMap<Integer, JSONArray> subspaceAttrMap 
				= new HashMap<Integer, JSONArray>();
		
		JSONArray attrArr1 = new JSONArray();
		attrArr1.put("attr1");
		attrArr1.put("attr2");
		attrArr1.put("attr3");
		
		
		JSONArray attrArr2 = new JSONArray();
		attrArr2.put("attr4");
		attrArr2.put("attr5");
		attrArr2.put("attr6");
		
		
		subspaceAttrMap.put(0, attrArr1);
		subspaceAttrMap.put(1, attrArr2);
		
		
		SubspaceBasedAnonymizedIDCreator anonymizedIDCreator 
						= new SubspaceBasedAnonymizedIDCreator(subspaceAttrMap);
		
		HashMap<String, List<ACLEntry>> aclMap = new HashMap<String, List<ACLEntry>>();
		
		List<ACLEntry> acl0 = new LinkedList<ACLEntry>();
		
		acl0.add(new ACLEntry(guidsVector.get(1).getGuid(), guidsVector.get(1).getPublicKey()));
		acl0.add(new ACLEntry(guidsVector.get(2).getGuid(), guidsVector.get(2).getPublicKey()));
		acl0.add(new ACLEntry(guidsVector.get(3).getGuid(), guidsVector.get(3).getPublicKey()));
		aclMap.put("attr1", acl0);
		
		
		List<ACLEntry> acl1 = new LinkedList<ACLEntry>();
		acl1.add(new ACLEntry(guidsVector.get(4).getGuid(), guidsVector.get(4).getPublicKey()));
		acl1.add(new ACLEntry(guidsVector.get(5).getGuid(), guidsVector.get(5).getPublicKey()));
		acl1.add(new ACLEntry(guidsVector.get(3).getGuid(), guidsVector.get(3).getPublicKey()));
		aclMap.put("attr2", acl1);
		
		
		List<ACLEntry> acl2 = new LinkedList<ACLEntry>();
		acl2.add(new ACLEntry(guidsVector.get(1).getGuid(), guidsVector.get(1).getPublicKey()));
		acl2.add(new ACLEntry(guidsVector.get(2).getGuid(), guidsVector.get(2).getPublicKey()));
		aclMap.put("attr3", acl2);
		
		
		List<ACLEntry> acl3 = new LinkedList<ACLEntry>();
		acl3.add(new ACLEntry(guidsVector.get(1).getGuid(), guidsVector.get(1).getPublicKey()));
		acl3.add(new ACLEntry(guidsVector.get(2).getGuid(), guidsVector.get(2).getPublicKey()));
		acl3.add(new ACLEntry(guidsVector.get(3).getGuid(), guidsVector.get(3).getPublicKey()));
		aclMap.put("attr4", acl3);
		
		
		List<ACLEntry> acl4 = new LinkedList<ACLEntry>();
		acl4.add(new ACLEntry(guidsVector.get(6).getGuid(), guidsVector.get(6).getPublicKey()));
		acl4.add(new ACLEntry(guidsVector.get(7).getGuid(), guidsVector.get(7).getPublicKey()));
		aclMap.put("attr5", acl4);
		
		
		List<ACLEntry> acl5 = new LinkedList<ACLEntry>();
		acl5.add(new ACLEntry(guidsVector.get(4).getGuid(), guidsVector.get(4).getPublicKey()));
		acl5.add(new ACLEntry(guidsVector.get(5).getGuid(), guidsVector.get(5).getPublicKey()));
		acl5.add(new ACLEntry(guidsVector.get(1).getGuid(), guidsVector.get(1).getPublicKey()));
		
		aclMap.put("attr6", acl5);
		
		
		List<AnonymizedIDEntry> anonymizedIdList = anonymizedIDCreator.computeAnonymizedIDs(aclMap);
		System.out.println("Number of anonymizedIds "+anonymizedIdList.size());
		
		System.out.println("\n\n\n##################################\n\n\n");
		for(int i=0;i<anonymizedIdList.size();i++)
		{
			AnonymizedIDEntry anonymizedEntry = anonymizedIdList.get(i);
			System.out.println(anonymizedEntry.toString());
		}
		
		System.out.println("\n\n\n##################################\n\n\n");
		
		System.out.println("\n Updating attr1 \n");
		
		HashMap<String, AttrValueRepresentationJSON> attrValueMap 
					= new HashMap<String, AttrValueRepresentationJSON>();
		
		AttrValueRepresentationJSON valRep = new AttrValueRepresentationJSON(10+"");
		
		attrValueMap.put("attr1", valRep);
		
		SubspaceBasedCSTransform csTransform = new SubspaceBasedCSTransform(Executors.newFixedThreadPool(1));
		List<CSUpdateTransformedMessage> transformedUpdateList = 
		csTransform.transformUpdateForCSPrivacy(guid0, attrValueMap, aclMap, anonymizedIdList);
		
		System.out.println("transformedList size "+transformedUpdateList.size());
		for(int i=0; i<transformedUpdateList.size(); i++)
		{
			CSUpdateTransformedMessage csTransMessage = transformedUpdateList.get(i);
			System.out.println(csTransMessage.toString());
		}
		
		// checking the untransform now.
		List<CSSearchReplyTransformedMessage> csTransformedSearchRepList
			= new LinkedList<CSSearchReplyTransformedMessage>();
		
		for(int i=0;i<transformedUpdateList.size();i++)
		{
			CSUpdateTransformedMessage csUpdateMessage = transformedUpdateList.get(i);
			String IDString = Utils.bytArrayToHex(csUpdateMessage.getAnonymizedID());
			// since we just updated attr1
			JSONArray realIDMapping = csUpdateMessage.getAttrValMap().get("attr1").getRealIDMappingInfo();
			
			SearchReplyGUIDRepresentationJSON searchRepJSON 
							= new SearchReplyGUIDRepresentationJSON(IDString, realIDMapping);
			CSSearchReplyTransformedMessage csSearchTransMesg 
												= new CSSearchReplyTransformedMessage(searchRepJSON);
			
			csTransformedSearchRepList.add(csSearchTransMesg);
		}
		
		JSONArray replyArray = new JSONArray();
		GuidEntry queryingGuid = guidsVector.get(1);
		csTransform.unTransformSearchReply(queryingGuid, csTransformedSearchRepList
				, replyArray);
		
		System.out.println("Query GUID "+ queryingGuid.getGuid()+
				" Real GUID "+guid0+" reply Arr "+replyArray);
		
		
		replyArray = new JSONArray();
		queryingGuid = guidsVector.get(4);
		csTransform.unTransformSearchReply(queryingGuid, csTransformedSearchRepList
				, replyArray);
		
		System.out.println("Query GUID "+ queryingGuid.getGuid()+
				" Real GUID "+guid0+" reply Arr "+replyArray);
	}
}