package edu.umass.cs.contextservice.test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import org.json.JSONArray;

import edu.umass.cs.contextservice.client.anonymizedID.AnonymizedIDCreationInterface;
import edu.umass.cs.contextservice.client.anonymizedID.HyperspaceBasedAnonymizedIDCreator;
import edu.umass.cs.contextservice.client.anonymizedID.SubspaceBasedAnonymizedIDCreator;
import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;

/**
 * This class tests number of anonymized IDs generated
 * when ACLs are generated from classes.
 * @author adipc
 */
public class AnonymizedIDWithClasses 
{
	// each class size is 5 guids.
	public static final int CLASS_SIZE		= 5;
	public static final int NUM_CLASSES		= 5;
	
	private final KeyPairGenerator kpg;
	private final Random aclRand;
	
	private final Vector<UserEntry> userVector;
	
	private final int numUsers;
	private final int numAttrs;
	
	private final AnonymizedIDCreationInterface anonymizedIDInterface;
	
	public AnonymizedIDWithClasses
					(int numUsers, int numAttrs) throws Exception
	{
		this.numUsers = numUsers;
		this.numAttrs = numAttrs;

//		subspaceAttrMap = new HashMap<Integer, JSONArray>();
//		int optimalH = 3;
//		int numberSubspaces = numAttrs/optimalH;
//
//		int numberAttrAssigned = 0;
//
//		while( numberAttrAssigned < numAttrs )
//		{
//			for( int i=0; i<numberSubspaces; i++ )
//			{
//				if( numberAttrAssigned >= numAttrs )
//				{
//					break;
//				}
//
//				JSONArray jsonArr = subspaceAttrMap.get(i);
//
//				if( jsonArr == null )
//				{
//					jsonArr = new JSONArray();
//					String attrName = "attr"+numberAttrAssigned;
//					jsonArr.put(attrName);
//					numberAttrAssigned++;
//					subspaceAttrMap.put(i, jsonArr);
//				}
//				else
//				{
//					String attrName = "attr"+numberAttrAssigned;
//					jsonArr.put(attrName);
//					numberAttrAssigned++;
//				}
//			}
//		}
//		System.out.println("Attribute assignment complete");

//		anonymizedIDInterface 
//			= new SubspaceBasedAnonymizedIDCreator(subspaceAttrMap);

		anonymizedIDInterface 
						= new HyperspaceBasedAnonymizedIDCreator();

		userVector = new Vector<UserEntry>();

		aclRand  = new Random(102);

		kpg = KeyPairGenerator.getInstance
			( "RSA" );
		//kpg.generateKeyPair();
		// just generate all user entries.
		//generateUserEntries();
	}
	
	private void generateUserEntries() throws Exception
	{
		System.out.println("generateUserEntries started "+numUsers);
		
		KeyPairGenerator kpg = KeyPairGenerator.getInstance( "RSA" );
		// generate guids
		for( int i=0; i < numUsers; i++ )
		{
			int guidNum = i;
			
			System.out.println("for i "+i);
			String alias = "GUID"+guidNum;
			KeyPair kp0 = kpg.generateKeyPair();
			PublicKey publicKey0 = kp0.getPublic();
			PrivateKey privateKey0 = kp0.getPrivate();
			byte[] publicKeyByteArray0 = publicKey0.getEncoded();
			
			String guid0 = GuidUtils.createGuidFromPublicKey(publicKeyByteArray0);
			GuidEntry myGUID = new GuidEntry(alias, guid0, 
					publicKey0, privateKey0);
			
			UserEntry userEntry = new UserEntry(myGUID);
			userVector.add(userEntry);
		}
		
		System.out.println("Guid creation complete");
		
		// generate ACLs.
		for( int i=0; i < numUsers; i++ )
		{
			UserEntry currUserEntry = userVector.get(i);
			
			int totalDistinctGuidsNeeded = CLASS_SIZE*NUM_CLASSES;
			
			HashMap<String, ACLEntry> distinctGuidMap 
									= new HashMap<String, ACLEntry>();
			
			// generate classes
			while( distinctGuidMap.size() != totalDistinctGuidsNeeded )
			{
				int randIndex = aclRand.nextInt( userVector.size() );
				
				GuidEntry randGuidEntry 
							= userVector.get(randIndex).getGuidEntry();
				byte[] guidACLMember 
							= Utils.hexStringToByteArray(randGuidEntry.getGuid());
				byte[] publicKeyBytes 
							= randGuidEntry.getPublicKey().getEncoded();
				
				ACLEntry aclEntry 
							= new ACLEntry(guidACLMember, publicKeyBytes);
				distinctGuidMap.put( Utils.bytArrayToHex(guidACLMember), 
						aclEntry );
				//unionACLEntry.add(aclEntry);
			}
			
			HashMap<Integer, List<ACLEntry>> aclClasses 
								= new HashMap<Integer, List<ACLEntry>>();
			
			Iterator<String> guidMapIter 
								= distinctGuidMap.keySet().iterator();
			
			int classnum = 0;
			int currClassSize = 0;
			
			List<ACLEntry> aclClassList = null;
			
			while( guidMapIter.hasNext() )
			{
				String guidACLMemberString = guidMapIter.next();
				ACLEntry currACLEntry = distinctGuidMap.get(guidACLMemberString);
				
				
				if( currClassSize == 0 )
				{
					aclClassList = new LinkedList<ACLEntry>();
					aclClassList.add(currACLEntry);
					currClassSize++;
				}
				else
				{
					aclClassList.add(currACLEntry);
					currClassSize++;
					
					if( currClassSize == CLASS_SIZE )
					{
						aclClasses.put(classnum, aclClassList);
						classnum++;
						currClassSize = 0;
					}
				}
			}
			currUserEntry.setACLClasses(aclClasses);
			
			
			// generate ACLs
			HashMap<String, List<ACLEntry>> aclMap 
						= new HashMap<String, List<ACLEntry>>();

			for( int j=0; j < numAttrs; j++ )
			{
				//List<ACLEntry> attrACLList 
				//						= new LinkedList<ACLEntry>();
				List<ACLEntry> attrACL  = new LinkedList<ACLEntry>();
				
				for( int k = 0; k < NUM_CLASSES; k++ )
				{
					double randVal = aclRand.nextDouble();
					
					// if less than 0.5, with half chance we pick 
					// this class for ACL
					if( randVal <= 0.5 )
					{
						List<ACLEntry> classMemberList = aclClasses.get(k);
						for( int l = 0; l < classMemberList.size(); l++ )
						{
							ACLEntry aclEntry = classMemberList.get(l);
							attrACL.add(aclEntry);
						}
					}
				}
				
				String attrName = "attr"+j;
				aclMap.put(attrName, attrACL);
			}
			currUserEntry.setACLMap( aclMap );
			
			
			// generate anonymized IDs
			
			List<AnonymizedIDEntry> anonymizedIDList = 
					anonymizedIDInterface.computeAnonymizedIDs(aclMap);
			
			if( anonymizedIDList != null )
			{
				System.out.println( "Number of anonymized IDs created "+
						anonymizedIDList.size() );
//				for(int k=0; k<anonymizedIDList.size(); k++)
//				{
//					System.out.println("UserNum"+i+" "+
//									anonymizedIDList.get(k).toString());
//				}
			}
			currUserEntry.setAnonymizedIDList(anonymizedIDList);
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		AnonymizedIDWithClasses obj 
						= new AnonymizedIDWithClasses(100, 20);
	
		obj.generateUserEntries();
	}
}