package edu.umass.cs.contextservice.client.csprivacytransform;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.client.anonymizedID.SubspaceBasedAnonymizedIDCreator;
import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gnsclient.client.GuidEntry;

public class HyperspaceBasedCSTransform implements CSPrivacyTransformInterface
{
	private final ExecutorService exectutorService;
	
	public HyperspaceBasedCSTransform()
	{
		this.exectutorService = Executors.newFixedThreadPool(200);
	}
	
	@Override
	public List<CSUpdateTransformedMessage> transformUpdateForCSPrivacy( String targetGuid, 
			JSONObject csAttrValPairs , HashMap<String, List<ACLEntry>> aclMap, 
			List<AnonymizedIDEntry> anonymizedIDList )
	{
		try
		{
			 List<CSUpdateTransformedMessage> transformedMesgList 
			 						= new LinkedList<CSUpdateTransformedMessage>();
			
			 HashMap<String, AnonymizedIDUpdateInfo> anonymizedIdToBeUpdateMap =
				computeAnonymizedIDsToBeUpdated( csAttrValPairs 
						, anonymizedIDList );
			 
			// now all the anonymized IDs and the attributes that needs to be updated
			// are calculated, 
			// just need to send out updates
			Iterator<String> anonymizedIter = anonymizedIdToBeUpdateMap.keySet().iterator();
			
			while( anonymizedIter.hasNext() )
			{
				String anonymizedIDString 	= anonymizedIter.next();
				//byte[] anonymizedIDBytes    = Utils.hexStringToByteArray(anonymizedIDString);
				AnonymizedIDUpdateInfo updateInfo 
											= anonymizedIdToBeUpdateMap.get(anonymizedIDString);
				
				// just for testing and debugging system.
				if(ContextServiceConfig.RAND_VAL_JSON)
				{
					//JSONObject randJSON = randomizeAttrValue( updateInfo.attrValPair );
				
					CSUpdateTransformedMessage transforMessage = new CSUpdateTransformedMessage
						(anonymizedIDString, updateInfo.attrValPair, 
									null);
					
					transformedMesgList.add(transforMessage);
				}
				else
				{
					CSUpdateTransformedMessage transforMessage = new CSUpdateTransformedMessage
							(anonymizedIDString, updateInfo.attrValPair, 
								updateInfo.anonymizedIDEntry.getAnonymizedIDToGUIDMapping());
					
					transformedMesgList.add(transforMessage);
				}
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
	public void unTransformSearchReply(GuidEntry myGuid, 
			List<CSSearchReplyTransformedMessage> csTransformedList
			, JSONArray replyArray)
	{	
		ParallelSearchReplyDecryption parallelSearchDecryption =
				new ParallelSearchReplyDecryption(myGuid , csTransformedList
						, replyArray, exectutorService);
		parallelSearchDecryption.doDecryption();
		
		if( ContextServiceConfig.DEBUG_MODE )
		{
//			System.out.println
//				("Total decryptions "+parallelSearchDecryption.getTotalDecryptionsOverall()
//				+" replyArray size "+replyArray.length());
		}
	}
	
	/**
	 * Computes a map that contains anonymized IDs that contains the attributes updated, 
	 * the guid set is computed by taking the intersection. The map is from the anonymized IDs to the set of attributes
	 * that needs to be updated for that anonymized ID, this set is subset of attrs 
	 * in attrValueMap.
	 * @param attrValueMap
	 * @param anonymizedIDList
	 * @return
	 * @throws JSONException 
	 */
	private HashMap<String, AnonymizedIDUpdateInfo> 
		computeAnonymizedIDsToBeUpdated( JSONObject attrValuePairs 
				, List<AnonymizedIDEntry> anonymizedIDList ) throws JSONException
	{
		HashMap<String, AnonymizedIDUpdateInfo> anonymizedIDsToBeUpdated 
			= new HashMap<String, AnonymizedIDUpdateInfo>();
		
		Iterator<String> attrIter = attrValuePairs.keys();

		while( attrIter.hasNext() )
		{
			String updAttr = attrIter.next();

			for( int i=0;i<anonymizedIDList.size();i++ )
			{
				AnonymizedIDEntry anonymizedIDEntry 
					= anonymizedIDList.get(i);
				
				boolean containsAttr = checkIfAttributeSetContainsGivenAttribute
											(anonymizedIDEntry.getAttributeMap(), updAttr);
				
				// only anonymized IDs that contain the updated attribute are updated.
				// otherwise there is a privacy leak.
				if( containsAttr )
				{
					String anonymizedIDString = anonymizedIDEntry.getID();
					AnonymizedIDUpdateInfo anonymizedIDUpdInfo 
						= anonymizedIDsToBeUpdated.get(anonymizedIDString);
	
					if( anonymizedIDUpdInfo == null )
					{
						anonymizedIDUpdInfo
							= new AnonymizedIDUpdateInfo(anonymizedIDEntry);
						anonymizedIDUpdInfo.attrValPair.put(updAttr, 
											attrValuePairs.getString(updAttr));
						anonymizedIDsToBeUpdated.put(anonymizedIDString, anonymizedIDUpdInfo);		
					}
					else
					{
						anonymizedIDUpdInfo.attrValPair.put(updAttr, 
								attrValuePairs.getString(updAttr));
					}
				}	
			}
		}
		return anonymizedIDsToBeUpdated;
	}
	
	/**
	 * This function returns true if the attribute set contains
	 * the given attribute.
	 * @return
	 */
	private boolean checkIfAttributeSetContainsGivenAttribute
									(HashMap<String, Boolean> attributeMap, String attrName)
	{
		return attributeMap.containsKey(attrName);
	}
	
	
	private class AnonymizedIDUpdateInfo
	{
		public AnonymizedIDEntry anonymizedIDEntry;
		public JSONObject attrValPair;
		
		public AnonymizedIDUpdateInfo(AnonymizedIDEntry anonymizedIDEntry)
		{
			this.anonymizedIDEntry = anonymizedIDEntry;
			attrValPair = new JSONObject();
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
		String guid0 = Utils.convertPublicKeyToGUIDString(publicKeyByteArray0);
		
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
			
			String guid = Utils.convertPublicKeyToGUIDString(publicKeyByteArray);
			
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
		
		
		List<AnonymizedIDEntry> anonymizedIdList 
						= anonymizedIDCreator.computeAnonymizedIDs(null, aclMap);
//		System.out.println("Number of anonymizedIds "+anonymizedIdList.size());
//		
//		System.out.println("\n\n\n##################################\n\n\n");
		for(int i=0;i<anonymizedIdList.size();i++)
		{
			AnonymizedIDEntry anonymizedEntry = anonymizedIdList.get(i);
//			System.out.println(anonymizedEntry.toString());
		}
		
//		System.out.println("\n\n\n##################################\n\n\n");
//		
//		System.out.println("\n Updating attr1 \n");
		
//		HashMap<String, AttrValueRepresentationJSON> attrValueMap 
//					= new HashMap<String, AttrValueRepresentationJSON>();
		
//		AttrValueRepresentationJSON valRep = new AttrValueRepresentationJSON(10+"");
		
//		attrValueMap.put("attr1", valRep);
		
//		SubspaceBasedCSTransform csTransform = new SubspaceBasedCSTransform(Executors.newFixedThreadPool(1));
//		List<CSUpdateTransformedMessage> transformedUpdateList = 
//		csTransform.transformUpdateForCSPrivacy(guid0, attrValueMap, aclMap, anonymizedIdList);
//		
//		System.out.println("transformedList size "+transformedUpdateList.size());
//		for(int i=0; i<transformedUpdateList.size(); i++)
//		{
//			CSUpdateTransformedMessage csTransMessage = transformedUpdateList.get(i);
//			System.out.println(csTransMessage.toString());
//		}
//		
//		// checking the untransform now.
//		List<CSSearchReplyTransformedMessage> csTransformedSearchRepList
//			= new LinkedList<CSSearchReplyTransformedMessage>();
//		
//		for(int i=0;i<transformedUpdateList.size();i++)
//		{
//			CSUpdateTransformedMessage csUpdateMessage = transformedUpdateList.get(i);
//			String IDString = Utils.bytArrayToHex(csUpdateMessage.getAnonymizedID());
//			// since we just updated attr1
//			JSONArray realIDMapping = csUpdateMessage.getAttrValMap().get("attr1").getRealIDMappingInfo();
//			
//			SearchReplyGUIDRepresentationJSON searchRepJSON 
//							= new SearchReplyGUIDRepresentationJSON(IDString, realIDMapping);
//			CSSearchReplyTransformedMessage csSearchTransMesg 
//												= new CSSearchReplyTransformedMessage(searchRepJSON);
//			
//			csTransformedSearchRepList.add(csSearchTransMesg);
//		}
//		
//		JSONArray replyArray = new JSONArray();
//		GuidEntry queryingGuid = guidsVector.get(1);
//		csTransform.unTransformSearchReply(queryingGuid, csTransformedSearchRepList
//				, replyArray);
//		
//		System.out.println("Query GUID "+ queryingGuid.getGuid()+
//				" Real GUID "+guid0+" reply Arr "+replyArray);
//		
//		
//		replyArray = new JSONArray();
//		queryingGuid = guidsVector.get(4);
//		csTransform.unTransformSearchReply(queryingGuid, csTransformedSearchRepList
//				, replyArray);
//		
//		System.out.println("Query GUID "+ queryingGuid.getGuid()+
//				" Real GUID "+guid0+" reply Arr "+replyArray);
	}
}