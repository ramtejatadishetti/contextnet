package edu.umass.cs.contextservice.client.anonymizedID;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import org.json.JSONArray;

import edu.umass.cs.contextservice.client.ContextClientInterfaceWithPrivacy;
import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gnsclient.client.GuidEntry;

/**
 * This class creates anonymized IDs based on guids in the ACL.
 * Each distinct guid in the ACL is assigned a different anonymized
 * ID. This is not used in the system, it is only used for experiments.
 * @author adipc
 *
 */
public class GUIDBasedAnonymizedIDCreator 
						implements AnonymizedIDCreationInterface
{
	private final Random rand = new Random();
	
	@Override
	public List<AnonymizedIDEntry> computeAnonymizedIDs
	(GuidEntry myGuidEntry, HashMap<String, List<ACLEntry>> aclMap )
	{
		HashMap<String, JSONArray> guidToAttrSetMap = 
				getGuidToAttributeSetMap(aclMap);
		
		List<AnonymizedIDEntry> anonymizedIDList 
									= new LinkedList<AnonymizedIDEntry>();
		
		
		Iterator<String> guidIter = guidToAttrSetMap.keySet().iterator();
		
		while( guidIter.hasNext() )
		{
			String guidString = guidIter.next();
			byte[] guidBytes = Utils.hexStringToByteArray(guidString);
			
			JSONArray attrSet = guidToAttrSetMap.get(guidString);
			
			
			byte[] anonymizedID 
						= new byte[ContextClientInterfaceWithPrivacy.SIZE_OF_ANONYMIZED_ID];
			
			rand.nextBytes(anonymizedID);
			
			// only self in guid set, as 1 anonymized ID per guid
			JSONArray guidSet = new JSONArray();
			guidSet.put(guidBytes);
			
			
			AnonymizedIDEntry anonymizedIDEntry 
						= new AnonymizedIDEntry(anonymizedID , attrSet, guidSet, null);
			
			anonymizedIDList.add(anonymizedIDEntry);
		}
		return anonymizedIDList;
	}
	
	/**
	 * Computes guid to attribute set map.
	 * Helper for anonymized ID computation.
	 * @param aclList
	 * @return
	 */
	private HashMap<String, JSONArray> getGuidToAttributeSetMap
								(HashMap<String, List<ACLEntry>> aclMap)
	{
		HashMap<String, JSONArray> guidToAttributeSetMap 
							= new HashMap<String, JSONArray>();
		
		Iterator<String> attrIter = aclMap.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String currAttrName = attrIter.next();
			
			List<ACLEntry> currACLList 
								= aclMap.get(currAttrName);
			
			for( int i = 0; i < currACLList.size(); i++ )
			{
				ACLEntry aclEntry = currACLList.get(i);
				String guidString = Utils.bytArrayToHex(aclEntry.getACLMemberGUID());
				
				JSONArray attrSet = guidToAttributeSetMap.get(guidString);
				
				if( attrSet == null )
				{
					attrSet = new JSONArray();
					attrSet.put(currAttrName);
					guidToAttributeSetMap.put(guidString, attrSet);
				}
				else
				{
					attrSet.put(currAttrName);
				}
			}
		}
		return guidToAttributeSetMap;
	}
	
	
	// testing the class.
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
		
		
		GUIDBasedAnonymizedIDCreator anonymizedIDCreator 
						= new GUIDBasedAnonymizedIDCreator();
		
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
		
		
		List<AnonymizedIDEntry> anonymizedIds 
			= anonymizedIDCreator.computeAnonymizedIDs(null, aclMap);
		System.out.println("Number of anonymizedIds "+anonymizedIds.size());
		
		System.out.println("\n\n\n##################################\n\n\n");
		
		for( int i=0; i<anonymizedIds.size(); i++ )
		{
			AnonymizedIDEntry anonymizedEntry = anonymizedIds.get(i);
			System.out.println( anonymizedEntry.toString() );
		}
	}
}