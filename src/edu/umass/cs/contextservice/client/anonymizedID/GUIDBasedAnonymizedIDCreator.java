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
import org.json.JSONException;

import edu.umass.cs.contextservice.client.ContextClientInterfaceWithPrivacy;
import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
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
		(GuidEntry myGuidEntry, HashMap<String, List<ACLEntry>> aclMap ) throws JSONException
	{
		HashMap<String, HashMap<String, Boolean>> guidToAttributesMap 
										= computeGuidToAttributesMap(aclMap);
		
		List<AnonymizedIDEntry> anonymizedIDList 
									= new LinkedList<AnonymizedIDEntry>();
		
		HashMap<String, ACLEntry> unionGuidsMap = 
				getUnionOfDistinctGuidsInACLMap(aclMap);
		
		Iterator<String> guidIter = guidToAttributesMap.keySet().iterator();
		
		while( guidIter.hasNext() )
		{
			String guidString = guidIter.next();
			byte[] guidBytes = Utils.hexStringToByteArray(guidString);
			
			HashMap<String, Boolean> attrMap = guidToAttributesMap.get(guidString);
			
			
			JSONArray attrSet = new JSONArray();
			
			Iterator<String> attrMapIter = attrMap.keySet().iterator();
			
			while(attrMapIter.hasNext())
			{
				attrSet.put(attrMapIter.next());
			}
			
			byte[] anonymizedID 
						= new byte[ContextClientInterfaceWithPrivacy.SIZE_OF_ANONYMIZED_ID];
			
			rand.nextBytes(anonymizedID);
			
			// only self in guid set, as 1 anonymized ID per guid
			JSONArray guidSet = new JSONArray();
			guidSet.put(guidBytes);
			
			JSONArray anonymizedIDToGuidMapping = 
					computeAnonymizedIDToGUIDMapping(myGuidEntry, guidSet, unionGuidsMap);
			
			AnonymizedIDEntry anonymizedIDEntry 
						= new AnonymizedIDEntry(anonymizedID , attrSet, guidSet, anonymizedIDToGuidMapping);
			
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
	private HashMap<String, HashMap<String, Boolean>> 
	computeGuidToAttributesMap(
			HashMap<String, List<ACLEntry>> aclMap) throws JSONException
	{
		// String is GUIDString
		// solving the non unique Guids in an attribute's ACL by taking 
		// a map of attributes here.
		HashMap<String, HashMap<String, Boolean>> guidToAttributesMap 
						= new HashMap<String, HashMap<String, Boolean>>();
		
		Iterator<String> attrNameIter = aclMap.keySet().iterator();
		//for( int i=0; i<attrArray.length(); i++ )
		while(attrNameIter.hasNext())
		{
			String currAttr = attrNameIter.next();
			
			ContextServiceLogger.getLogger().fine(" currAttr "+currAttr);
			
			List<ACLEntry> attrACL = aclMap.get(currAttr);
			//List<byte[]> publicKeyACLMembers = attrACL.getPublicKeyACLMembers();
			
			for( int j=0; j<attrACL.size(); j++ )
			{
				//byte[] publicKeyByteArray = (byte[]) attrACL.get(j).getPublicKeyACLMember();
				byte[] guidByteArray = attrACL.get(j).getACLMemberGUID();
				String guidString = Utils.bytArrayToHex(guidByteArray);
						
				ContextServiceLogger.getLogger().fine
				(" currAttr "+currAttr+" guid "+guidString);
				
				HashMap<String, Boolean> attrMapBgm 
									= guidToAttributesMap.get(guidString);
				
				if( attrMapBgm == null )
				{
					attrMapBgm = new HashMap<String, Boolean>();
					attrMapBgm.put(currAttr, true);
					guidToAttributesMap.put(guidString, attrMapBgm);
				}
				else
				{
					attrMapBgm.put(currAttr, true);
				}
			}
		}
		ContextServiceLogger.getLogger().fine( "Size of guidToAttributesMap "
				+guidToAttributesMap.size() );
		return guidToAttributesMap;
	}
	
	private HashMap<String, ACLEntry> getUnionOfDistinctGuidsInACLMap
	(HashMap<String , List<ACLEntry>> aclMap)
	{
		HashMap<String, ACLEntry> unionACLMap 
		= new HashMap<String, ACLEntry>();
		
		Iterator<String> attrIter = aclMap.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			List<ACLEntry>  aclList = aclMap.get(attrName);
			
			for(int i=0; i<aclList.size(); i++)
			{
				ACLEntry currACL = aclList.get(i);
				String currGUID  = Utils.bytArrayToHex(currACL.getACLMemberGUID());
				unionACLMap.put(currGUID, currACL);
			}
			//= aclMap.get(key);
		}
		return unionACLMap;
	}
	
	/**
	 * this function computes the anonymized ID to GuidMapping.
	 * It also stores the encryption info for each guid in guid set 
	 * accorind to its hash value.
	 * @return
	 */
	private JSONArray computeAnonymizedIDToGUIDMapping( GuidEntry guidEntry, 
			JSONArray guidSet, HashMap<String, ACLEntry> unionGuidsMap )
	{
		JSONArray anonymizedIDToGuidMapping = new JSONArray();
		byte[] userGuidBytes = Utils.hexStringToByteArray(guidEntry.getGuid());
		List<String> notAssignedGuids = new LinkedList<String>();
		
		for(int i=0; i<guidSet.length(); i++)
		{
			try 
			{
				byte[] guidBytes = (byte[]) guidSet.get(i);
				String guidString = Utils.bytArrayToHex(guidBytes);
				int index = Utils.consistentHashAString(guidString, guidSet.length());
				
				// place free insert now
				if(anonymizedIDToGuidMapping.isNull(index))
				{
					ACLEntry currACLEntry = unionGuidsMap.get(guidString);
					byte[] encryptedInfo = Utils.doPublicKeyEncryption(currACLEntry.getPublicKeyACLMember(), userGuidBytes);
					// store it in JSON
					anonymizedIDToGuidMapping.put(index, Utils.bytArrayToHex(encryptedInfo));
				}
				else
				{
					// save it for second round of assignment
					notAssignedGuids.add(guidString);
				}
			}
			catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
//		System.out.println("Number of guids in ACL not stored in correct localtion "
//								+notAssignedGuids+" total length "+guidSet.length());
		
		// assign remaining ones
		// are assigned any location free after their hashed location.
		for(int i=0 ;i<notAssignedGuids.size(); i++)
		{
			try
			{
				String guidString = notAssignedGuids.get(i);
				int index = Utils.consistentHashAString(guidString, guidSet.length());
				int newIndex = getNextFreeIndex(index, anonymizedIDToGuidMapping, 
						guidSet.length());
				
				assert(anonymizedIDToGuidMapping.isNull(newIndex));
				
				ACLEntry currACLEntry = unionGuidsMap.get(guidString);
				byte[] encryptedInfo = Utils.doPublicKeyEncryption
						(currACLEntry.getPublicKeyACLMember(), 
						userGuidBytes);
				// store it in JSON
				anonymizedIDToGuidMapping.put(newIndex, Utils.bytArrayToHex(encryptedInfo));
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
			
		for(int i=0; i<anonymizedIDToGuidMapping.length(); i++)
		{
			assert(!anonymizedIDToGuidMapping.isNull(i));
		}
		assert(anonymizedIDToGuidMapping.length() == guidSet.length());
		return anonymizedIDToGuidMapping;
	}
	
	private int getNextFreeIndex(int startIndex, JSONArray anonymizedIDToGuidMapping, 
			int guidSetLength)
	{	
		int currIndex = startIndex%guidSetLength;
		while(true)
		{
			if(!anonymizedIDToGuidMapping.isNull(currIndex))
			{
				currIndex++;
				currIndex = currIndex%guidSetLength;
				continue;
			}
			else
			{
				return currIndex;
			}
		}
	}
	
	
	// testing the class.
	public static void main(String[] args) throws NoSuchAlgorithmException, JSONException
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
			= anonymizedIDCreator.computeAnonymizedIDs(myGUID, aclMap);
//		System.out.println("Number of anonymizedIds "+anonymizedIds.size());
//		
//		System.out.println("\n\n\n##################################\n\n\n");
		
		for( int i=0; i<anonymizedIds.size(); i++ )
		{
			AnonymizedIDEntry anonymizedEntry = anonymizedIds.get(i);
//			System.out.println( anonymizedEntry.toString() );
		}
	}
}