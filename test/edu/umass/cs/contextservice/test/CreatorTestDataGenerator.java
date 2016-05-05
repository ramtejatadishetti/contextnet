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
import edu.umass.cs.contextservice.client.anonymizedID.SubspaceBasedAnonymizedIDCreator;
import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;

public class CreatorTestDataGenerator
{
	public static final int UNION_ACL_SIZE 				= 100;
	public static final int ACL_SIZE					= 10;
	
	private final KeyPairGenerator kpg;
	private final Random aclRand;
	
	private final int numUsers;
	private final int numAttrs;
	
	private final Vector<UserEntry> userVector;
	
	private final HashMap<Integer, JSONArray> subspaceAttrMap;
	
	private final AnonymizedIDCreationInterface anonymizedIDInterface;
	
	public CreatorTestDataGenerator
						(int numUsers, int numAttrs) throws Exception
	{
		this.numUsers = numUsers;
		this.numAttrs = numAttrs;
		
		subspaceAttrMap = new HashMap<Integer, JSONArray>();
		
		int optimalH = 3;
		int numberSubspaces = numAttrs/optimalH;
		
		int numberAttrAssigned = 0;
		
		while( numberAttrAssigned < numAttrs )
		{
			for( int i=0; i<numberSubspaces; i++ )
			{
				if( numberAttrAssigned >= numAttrs )
				{
					break;
				}
				
				JSONArray jsonArr = subspaceAttrMap.get(i);
				
				if( jsonArr == null )
				{
					jsonArr = new JSONArray();
					String attrName = "attr"+numberAttrAssigned;
					jsonArr.put(attrName);
					numberAttrAssigned++;
					subspaceAttrMap.put(i, jsonArr);
				}
				else
				{
					String attrName = "attr"+numberAttrAssigned;
					jsonArr.put(attrName);
					numberAttrAssigned++;
				}
			}
		}
		
		System.out.println("Attribute assignment complete");
		
		anonymizedIDInterface 
			= new SubspaceBasedAnonymizedIDCreator(subspaceAttrMap);
		
//		anonymizedIDInterface 
//				= new HyperspaceBasedAnonymizedIDCreator();
		
		userVector = new Vector<UserEntry>();
		
		aclRand  = new Random(102);
		
		kpg = KeyPairGenerator.getInstance
					( "RSA" );
		//kpg.generateKeyPair();
		// just generate all user entries.
		//generateUserEntries();
	}
	
	/**
	 * All things happen in this function are local
	 * no cs communication so no need for rate control.
	 * @throws Exception
	 */
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
			// there is a map to have unique 20 elements
			HashMap<String, ACLEntry> unionACLEntryMap 
								= new HashMap<String, ACLEntry>();
			
			//List<ACLEntry> unionACLEntry = new LinkedList<ACLEntry>();
			while( unionACLEntryMap.size() != UNION_ACL_SIZE )
			{
				int randIndex = aclRand.nextInt( userVector.size() );
				
				GuidEntry randGuidEntry 
						= userVector.get(randIndex).getGuidEntry();
				byte[] guidACLMember = Utils.hexStringToByteArray(randGuidEntry.getGuid());
				byte[] publicKeyBytes = randGuidEntry.getPublicKey().getEncoded();
				
				ACLEntry aclEntry = new ACLEntry(guidACLMember, publicKeyBytes);
				unionACLEntryMap.put( Utils.bytArrayToHex(guidACLMember), 
						aclEntry );
				//unionACLEntry.add(aclEntry);
			}			
			
			currUserEntry.setUnionOfACLs(unionACLEntryMap);
			
			// generate ACLs by picking 10 random entries 
			// from the union of ACLs for each attribute.
			HashMap<String, List<ACLEntry>> aclMap 
								= new HashMap<String, List<ACLEntry>>();
			
			for( int j=0; j < numAttrs; j++ )
			{
				//List<ACLEntry> attrACLList 
				//						= new LinkedList<ACLEntry>();
				String[] guidArray = new String[unionACLEntryMap.size()];
				
				guidArray = unionACLEntryMap.keySet().toArray(guidArray);
				
				HashMap<String, ACLEntry> attrACLMap 
										= new HashMap<String, ACLEntry>();
				
				while( attrACLMap.size() != ACL_SIZE )
				{
					int randIndex = aclRand.nextInt( guidArray.length );
					ACLEntry aclEntry = unionACLEntryMap.get(guidArray[randIndex]);
					
					attrACLMap.put(Utils.bytArrayToHex(aclEntry.getACLMemberGUID()), aclEntry);
				}
				
				Iterator<String> guidIter = attrACLMap.keySet().iterator();
				List<ACLEntry> aclList = new LinkedList<ACLEntry>();
				while( guidIter.hasNext() )
				{
					String guidStr = guidIter.next();
					aclList.add(attrACLMap.get(guidStr));
				}
				
				String attrName = "attr"+j;
				
				aclMap.put(attrName, aclList);
			}
			currUserEntry.setACLMap( aclMap );
			
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
	
	public static void main( String[] args ) throws Exception
	{
		CreatorTestDataGenerator test 
					= new CreatorTestDataGenerator(100, 20);
		test.generateUserEntries();
	}
}