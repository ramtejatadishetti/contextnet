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
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gnsclient.client.GuidEntry;


/**
 * Implements the subspace based anonymized creator,
 * this scheme is used in the system. It implements the best heuristic 
 * we have for minimal anonymized ID creation.
 * 
 * This anonymized ID creator is depracated and wrong.
 * HyperspaceBasedAnonymizedIDCreator is used.
 * @author adipc
 */
public class SubspaceBasedAnonymizedIDCreator 
									implements AnonymizedIDCreationInterface
{
	private final HashMap<Integer, JSONArray> subspaceAttrMap;
	private final Random anonymizedIDRand					= new Random();
	
	public SubspaceBasedAnonymizedIDCreator(HashMap<Integer, JSONArray> subspaceAttrMap)
	{
		this.subspaceAttrMap = subspaceAttrMap;
	}
	
	@Override
	public List<AnonymizedIDEntry> computeAnonymizedIDs
							( GuidEntry myGuidEntry, HashMap<String, List<ACLEntry>> aclMap ) 
	{
		//TODO: check List<ACLEntry> doesn't contain repeated guids, that causes more anonymized Ids to be generated
		// and 2^H max num of anonymized IDs for a subspace also gets violated.
		
		try
		{
			// each element is AnonymizedIDStoringClass
			List<AnonymizedIDEntry> anonymizedIDList 
								= new LinkedList<AnonymizedIDEntry>();
			ContextServiceLogger.getLogger().fine("Size of attrACLMap "+aclMap.size() );
			
			Iterator<Integer> subspaceIter 
										= this.subspaceAttrMap.keySet().iterator();
			
			while( subspaceIter.hasNext() )
			{
				int mapKey = subspaceIter.next();
				
				JSONArray attrArray = this.subspaceAttrMap.get(mapKey);
				
				System.out.println("subspace attrs "+attrArray);
				
				// String is GUIDString
				HashMap<String, List<String>> guidToAttributesMap 
								= computeGuidToAttributesMap(attrArray, aclMap);
				
				//printGuidToAttributesMap( guidToAttributesMap );
				
				// guidToAttributesMap computed now compute anonymized IDs
				// we sort the list of attributes, so that different permutations of same set 
				// becomes same and then just add them to hashmap for finding distinct sets.
				
				// JSONArray of attributes is the String.
				// JSONArray cannot be directly used.
				HashMap<String, JSONArray> attributesToGuidsMap 
						= computeAttributesToGuidsMap(guidToAttributesMap);
			
				// apply minimization heursitic
				HashMap<String, JSONArray> minimizedAttrSet = removeRedundantAnonymizedIDs
				( attributesToGuidsMap );
				
				System.out.println("Reduction from minimization before "+
						attributesToGuidsMap.size()+" after "+minimizedAttrSet.size());
				// now assign anonymized ID
				//HashMap<String, List<byte[]>> attributesToGuidsMap 
				//	= new HashMap<String, List<byte[]>>();

				Iterator<String> attrSetIter = minimizedAttrSet.keySet().iterator();
				
				while( attrSetIter.hasNext() )
				{
					// JSONArray in string format
					String key = attrSetIter.next();
					JSONArray attrSet = new JSONArray(key);
					JSONArray guidSet = minimizedAttrSet.get(key);
					assert(attrSet != null);
					assert(guidSet != null);
					
					byte[] anonymizedID 
								= new byte[ContextClientInterfaceWithPrivacy.SIZE_OF_ANONYMIZED_ID];
					
					anonymizedIDRand.nextBytes(anonymizedID);
					
					AnonymizedIDEntry anonymizedIDObj 
						= new AnonymizedIDEntry(anonymizedID, attrSet, guidSet, null);
					
					
					anonymizedIDList.add(anonymizedIDObj);
				}
			}
			return anonymizedIDList;
		}
		catch( JSONException jsoEx )
		{
			jsoEx.printStackTrace();
		}
		return null;
	}
	
	/**
	 * computes the guid to attributes map for each member fo ACL.
	 * Key is the guid and value is the list of attributes that the guid is
	 * allowed to read.
	 * It is Gk:Buk map in the draft.
	 * @throws JSONException 
	 */
	private HashMap<String, List<String>> 
			computeGuidToAttributesMap(JSONArray attrArray, 
					HashMap<String, List<ACLEntry>> aclMap) throws JSONException
	{
		// String is GUIDString
		HashMap<String, List<String>> guidToAttributesMap 
						= new HashMap<String, List<String>>();
		
		for( int i=0; i<attrArray.length(); i++ )
		{
			String currAttr = attrArray.getString(i);
			
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
				
				List<String> attrListBuk = guidToAttributesMap.get(guidString);
				
				if( attrListBuk == null )
				{
					attrListBuk = new LinkedList<String>();
					attrListBuk.add(currAttr);
					guidToAttributesMap.put(guidString, attrListBuk);
				}
				else
				{
					attrListBuk.add(currAttr);
				}
			}
		}
		
		ContextServiceLogger.getLogger().fine( "Size of guidToAttributesMap "
				+guidToAttributesMap.size() );
		return guidToAttributesMap;
	}
	
	
	/**
	 * This computes the map from set of attributes to set of Guids, all the guids
	 * are allowed to read the set of attributes in the key.
	 * Set of attributes is the sorted JSONArray String represntation.
	 * As JSONArray cannot be directly used as map key.
	 * Set of guids is the JSONArray, where each element is byte[]
	 * 
	 * This is the Bul:Gul map in the draft.
	 * @return
	 */
	private HashMap<String, JSONArray> computeAttributesToGuidsMap(
			HashMap<String, List<String>> guidToAttributesMap)
	{
		HashMap<String, JSONArray> attributesToGuidsMap 
									= new HashMap<String, JSONArray>();

		Iterator<String> guidIter = guidToAttributesMap.keySet().iterator();

		while( guidIter.hasNext() )
		{
			String currGUIDString = guidIter.next();
			byte[] currGUID = Utils.hexStringToByteArray(currGUIDString);
			ContextServiceLogger.getLogger().fine(" printing guidToAttributesMap keys "
					+currGUIDString);

			List<String> attrListBuk = guidToAttributesMap.get(currGUIDString);
			// sorting by natural String order
			attrListBuk.sort(null);

			// it is just concantenation of attrs in sorted order
			JSONArray attrArrayBul = new JSONArray();
			for( int i=0; i < attrListBuk.size(); i++)
			{
				attrArrayBul.put(attrListBuk.get(i));
			}

			ContextServiceLogger.getLogger().fine(" printing attrArrayBul"
					+attrArrayBul.toString()+" currGUIDString "+currGUIDString);

			JSONArray guidsListArray = attributesToGuidsMap.get(attrArrayBul.toString());

			if( guidsListArray == null )
			{
				guidsListArray = new JSONArray();
				assert(currGUID != null);
				guidsListArray.put(currGUID);
				attributesToGuidsMap.put(attrArrayBul.toString(), guidsListArray);
				ContextServiceLogger.getLogger().fine("attrArrayBul "+attrArrayBul.toString()
				+" guidsListArray "+guidsListArray.length());
			}
			else
			{
				assert(currGUID != null);
				guidsListArray.put(currGUID);
				ContextServiceLogger.getLogger().fine("attrArrayBul "+attrArrayBul.toString()
				+" guidsListArray "+guidsListArray.length());
			}
		}
		
		ContextServiceLogger.getLogger().fine("Size of attributesToGuidsMap "
							+attributesToGuidsMap.size() );	
		return attributesToGuidsMap;
	}
	
	
	/**
	 * This function minimizes the anononymized IDs,
	 * It first tries to remove anonymized IDs whose corresponding set
	 * is of length 1 and then proceeds to length 2, and so on.
	 * 
	 * @param attributesToGuidsMap
	 * @return
	 */
	private HashMap<String, JSONArray> removeRedundantAnonymizedIDs
									( HashMap<String, JSONArray> attributesToGuidsMap )
	{
		HashMap<String, JSONArray> afterRemoval = new HashMap<String, JSONArray>();
		
		// store attribute sets based on length.
		// key is the attr set length and 
		// the value is the list of such sets, each set is represented by JSONArray
		HashMap<Integer, List<JSONArray>> lengthIndexedAttrSets 
									= new HashMap<Integer, List<JSONArray>>();
		
		Iterator<String> attrsToGuidsMapIter = attributesToGuidsMap.keySet().iterator();
		
		int minLength = -1;
		int maxLength = -1;
		
		while( attrsToGuidsMapIter.hasNext() )
		{
			String jsonArrayString = attrsToGuidsMapIter.next();
			try
			{
				JSONArray attrSetArray = new JSONArray(jsonArrayString);
				int arrLength = attrSetArray.length();
				
				if( minLength == -1 )
				{
					minLength = arrLength;
				}
				else
				{
					if( arrLength < minLength )
					{
						minLength = arrLength;
					}
				}
				
				if( maxLength == -1 )
				{
					maxLength = arrLength;
				}
				else
				{
					if( arrLength > maxLength )
					{
						maxLength = arrLength;
					}
				}
				
				
				List<JSONArray> currArrList = lengthIndexedAttrSets.get(arrLength);
				
				if( currArrList == null )
				{
					currArrList = new LinkedList<JSONArray>();
					currArrList.add(attrSetArray);
					lengthIndexedAttrSets.put(arrLength, currArrList);
				}
				else
				{
					currArrList.add(attrSetArray);
				}
			} catch ( JSONException e )
			{
				e.printStackTrace();
			}
		}
		
		// the heuristic removes the redundant sets from 
		// minimum length to the maximum length
		for( int i=minLength; i <= maxLength; i++ )
		{
			List<JSONArray> attrSetList = lengthIndexedAttrSets.get(i);
			
			// all attr set length may not be there
			if( attrSetList != null )
			{
				for( int j=0; j<attrSetList.size() ; j++ )
				{
					JSONArray currAttrSet = attrSetList.get(j);
					
					List<JSONArray> superSetList = 
							getSuperSetsOfASet( lengthIndexedAttrSets , 
									currAttrSet );
					
					JSONArray currGuidSet = attributesToGuidsMap.get(currAttrSet.toString());
					
					assert(currGuidSet != null);
					
					boolean inUnion = checkIfGuidSetContainedInUnion( currGuidSet, 
							attributesToGuidsMap , superSetList );
					
					// if not in the union then it should be included, as it is not redundant
					if( !inUnion )
					{
						afterRemoval.put
								(currAttrSet.toString(), currGuidSet);
					}
				}
			}
		}
		return afterRemoval;
	}
	
	
	/**
	 * Returns the list of sets that are 
	 * supersets of the current set.
	 * 
	 * @param lengthIndexedAttrSets
	 * @param currSet
	 * @return
	 */
	private List<JSONArray> getSuperSetsOfASet( HashMap<Integer, List<JSONArray>> lengthIndexedAttrSets , 
								JSONArray currSet )
	{
		int setLength = currSet.length();
		
		List<JSONArray> superSetList =  new LinkedList<JSONArray>();
		
		Iterator<Integer> mapIter = lengthIndexedAttrSets.keySet().iterator();
		
		while( mapIter.hasNext() )
		{
			int currLen = mapIter.next();
			
			// a superset can be a set with higher length.
			if( currLen > setLength )
			{
				
				List<JSONArray> toCheckList = lengthIndexedAttrSets.get(currLen);
				
				for( int i=0; i<toCheckList.size(); i++ )
				{
					JSONArray checkForSuperSet = toCheckList.get(i);
					
					if( checkForSuperset(currSet, checkForSuperSet) )
					{
						superSetList.add(checkForSuperSet);
					}
				}
				//superSetList.
			}
		}
		return superSetList;
	}
	
	
	/**
	 * Checks if the second set is the superset of the first
	 * set. JSONArrays are the attribute strings
	 * @return
	 */
	private boolean checkForSuperset(JSONArray subset, JSONArray superset)
	{
		HashMap<String, Boolean> mapToCheck = new HashMap<String, Boolean>();
		
		for( int i=0; i < superset.length(); i++)
		{
			try 
			{
				String attrName = superset.getString(i);
				mapToCheck.put(attrName, true);
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		boolean isSubset = true;
		
		for( int i=0; i < subset.length(); i++ )
		{
			try 
			{
				String attrname = subset.getString(i);
				if( !mapToCheck.containsKey(attrname) )
				{
					isSubset = false;
					break;
				}
			} 
			catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
		
		return isSubset;
	}
	
	/**
	 * Checks if the guid set of an attribute set is containied in the
	 * union of super sets.
	 * gudset is in byte[] 
	 * @return
	 */
	private boolean checkIfGuidSetContainedInUnion( JSONArray guidSet, 
			HashMap<String, JSONArray> attributesToGuidsMap , List<JSONArray> superSetList )
	{
		// denotes the union of of guid sets of all attribute sets in 
		// in the superlist
		HashMap<String, Boolean> unionOfGuidSets = new HashMap<String, Boolean>();
		
		for( int i=0; i < superSetList.size(); i++ )
		{
			JSONArray currAttrArray = superSetList.get(i);
			String jsonArrString = currAttrArray.toString();
			
			// each element of this JSONArray is a byte[]
			JSONArray guidSetArr = attributesToGuidsMap.get(jsonArrString);
			
			assert(guidSetArr != null);
			
			for( int j=0; j < guidSetArr.length(); j++ )
			{
				try
				{
					byte[] guidBytes = (byte[]) guidSetArr.get(j);
					String guidString = Utils.bytArrayToHex(guidBytes);
					unionOfGuidSets.put(guidString, true);
				} 
				catch (JSONException e) 
				{
					e.printStackTrace();
				}
			}
		}
		
		boolean inUnuion = true;
		
		for(int i=0 ; i<guidSet.length(); i++)
		{
			try 
			{
				byte[] guidBytes = (byte[])guidSet.get(i);
				String guidString = Utils.bytArrayToHex(guidBytes);
				
				if( !unionOfGuidSets.containsKey(guidString) )
				{
					inUnuion = false;
					break;
				}
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
		return inUnuion;
	}
	
	
	private void printGuidToAttributesMap( HashMap<String, 
			List<String>> guidToAttributesMap )
	{
		Iterator<String> guidIter = guidToAttributesMap.keySet().iterator();
		//guidToAttributesMap
		while( guidIter.hasNext() )
		{
			String printStr = "";
			String guidStr = guidIter.next();
			
			List<String> attrList = guidToAttributesMap.get(guidStr);
			
			printStr = "Guid "+guidStr+" Attrs ";
			
			for( int i=0; i<attrList.size() ; i++ )
			{
				printStr = printStr + " , "+attrList.get(i);
			}
			System.out.println("printStr "+printStr);
		}
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
		
		
		List<AnonymizedIDEntry> anonymizedIds = anonymizedIDCreator.computeAnonymizedIDs(null, aclMap);
		System.out.println("Number of anonymizedIds "+anonymizedIds.size());
		
		System.out.println("\n\n\n##################################\n\n\n");
		
		for(int i=0;i<anonymizedIds.size();i++)
		{
			AnonymizedIDEntry anonymizedEntry = anonymizedIds.get(i);
			System.out.println( anonymizedEntry.toString() );
		}
	}
}