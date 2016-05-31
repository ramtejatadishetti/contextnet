package edu.umass.cs.contextservice.test;

import static org.junit.Assert.*;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.nodeApp.StartContextServiceNode;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gnsclient.client.GuidEntry;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ContextServiceTests 
{
	private static KeyPairGenerator kpg;
	private static ContextServiceClient<Integer> csClient = null;
	
	private static String memberAliasPrefix = "clientGUID";
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception 
	{
		kpg = KeyPairGenerator.getInstance("RSA");
		// context service config to use.
		ContextServiceConfig.configFileDirectory 
			= "/home/adipc/Documents/MobilityFirstGitHub/ContextNet/contextnet/conf/testConf/contextServiceConf"; 
		
		// start context service.
		startFourNodeSetup();
		
		String csNodeIp = "127.0.0.1";
		int csPort = 8000;
		
		// make a client connection
		csClient = new ContextServiceClient<Integer>(csNodeIp, csPort, 
				ContextServiceClient.HYPERSPACE_BASED_CS_TRANSFORM);
		
		System.out.println("ContextServiceClient connected");
	}
	
	@Test
	public void test_1_emptyQueryTest() 
	{
		String selectQuery = 
			"SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE "
			+ "attr1 >= 1 AND attr1 <= 1500 AND "
			+ "attr3 >= 1 AND attr3 <= 1500 AND "
			+ "attr5 >= 1 AND attr5 <= 1500";
		
		JSONArray replyArray = new JSONArray();
		long expiryTime = 300000; // 5 min
		int numRep = csClient.sendSearchQuery(selectQuery, replyArray, expiryTime);
		
		assertEquals(numRep, 0);
		assertEquals(replyArray.length(), 0);
	}
	
	@Test
	public void test_2_Input100GUIDs() throws JSONException 
	{
		Random rand = new Random();
		for(int i=0; i<100; i++)
		{
			String realAlias = memberAliasPrefix+i;
			String myGUID = getGUID(realAlias);
			JSONObject attrValJSON = getARandomAttrValSet();
			csClient.sendUpdate(myGUID, null, attrValJSON, -1, true);
		}
		String selectQuery = 
			"SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE "
			+ "attr1 >= 1 AND attr1 <= 1500 AND "
			+ "attr3 >= 1 AND attr3 <= 1500 AND "
			+ "attr5 >= 1 AND attr5 <= 1500";
		
		JSONArray replyArray = new JSONArray();
		long expiryTime = 300000; // 5 min
		int numRep = csClient.sendSearchQuery(selectQuery, replyArray, expiryTime);
		
		assertEquals(100, numRep);
		assertEquals(100, replyArray.length());
	}
	
	@Test
	public void test_3_privacyTest() throws JSONException, NoSuchAlgorithmException 
	{
		// if privacy not enabled then just return.
		if(!ContextServiceConfig.PRIVACY_ENABLED)
			return;
		
		GuidEntry userGUID = getAGUIDEntry("userGuid");
		
		Vector<GuidEntry> aclMemberGuids = new Vector<GuidEntry>();
		
		for( int i=0; i<10; i++ )
		{
			GuidEntry aclMem = getAGUIDEntry("aclMember"+i);
			aclMemberGuids.add(aclMem);
		}
		
		// create ACLs
		// 6 is the number of attributes.
		HashMap<String, List<ACLEntry>> aclMap 
								= new HashMap<String, List<ACLEntry>>();
		
		for(int i=0; i<6; i++)
		{
			String attrName = "attr"+i;
			List<ACLEntry> attrACL = new LinkedList<ACLEntry>();
			// even are assign first five acl guids
			if( (i % 2) == 0 )
			{
				for(int j=0; j<5; j++)
				{
					ACLEntry aclEntry 
						= new ACLEntry(aclMemberGuids.get(j).getGuid(), 
								aclMemberGuids.get(j).getPublicKey());
					attrACL.add(aclEntry);
					System.out.println("attrName "+attrName+" GUID "+
							aclMemberGuids.get(j).getGuid());
				}
			}
			// odd are assigned last five acl guids
			else if( (i % 2) == 1 )
			{
				for(int j=5; j<10; j++)
				{
					ACLEntry aclEntry 
						= new ACLEntry(aclMemberGuids.get(j).getGuid(), 
							aclMemberGuids.get(j).getPublicKey());
					attrACL.add(aclEntry);
					
					System.out.println("attrName "+attrName+" GUID "+
							aclMemberGuids.get(j).getGuid());
				}
			}
			
			// adding self
			
			ACLEntry aclEntry 
				= new ACLEntry(userGUID.getGuid(), 
						userGUID.getPublicKey());
			attrACL.add(aclEntry);
			
			aclMap.put(attrName, attrACL);
		}
		
		System.out.println("Size of "+aclMap.size());
		// compute anonymized IDs
		List<AnonymizedIDEntry> anonymizedIDsList 
					= csClient.computeAnonymizedIDs(userGUID, aclMap);
		
		for(int i=0; i< anonymizedIDsList.size(); i++)
		{
			System.out.println("anonymizedIDsList "+i+" "+anonymizedIDsList.get(i));
		}
		
		System.out.println("Number of anonymized IDs created "
											+anonymizedIDsList.size());
		assertTrue(anonymizedIDsList!=null);
		assertTrue(anonymizedIDsList.size() > 0);
		
		// number of anonymized IDs cannot be greater then total
		// distinct ACL GUIDs.
		assertTrue((aclMemberGuids.size()+1) >= anonymizedIDsList.size());
		//assertEquals(101, numRep);
		
		
		// do a secure update
		JSONObject attrValJSON = getARandomAttrValSet();
		
		csClient.sendUpdateSecure
			(userGUID.getGuid(), userGUID, attrValJSON, 
					-1, true, aclMap, anonymizedIDsList);
		
		
		for(int i=0; i<10; i++)
		{
			String selectQuery = 
					"SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE "
					+ "attr0 >= 2 AND attr0 <= 1500 AND "
					+ "attr2 >= 2 AND attr2 <= 1500 AND "
					+ "attr4 >= 2 AND attr4 <= 1500";
				
			JSONArray replyArray = new JSONArray();
			long expiryTime = 300000; // 5 min
			
			int numRep = csClient.sendSearchQuerySecure
					(selectQuery, replyArray, expiryTime, aclMemberGuids.get(i));
			
			// querier is allowed to read
			if(i<5)
			{
				assertTrue(userGUID.getGuid().compareToIgnoreCase(replyArray.getString(0)) == 0  );
				assertEquals(1, numRep);
				assertEquals(1, replyArray.length());
			}
			else
			{
				//assertTrue(replyArray.get(0).equals(userGUID.getGuid()));
				assertEquals(0, numRep);
				assertEquals(0, replyArray.length());
			}
		}
		
		// across two different sets
		// nobody except from userGUID should be allowed to read.
		for(int i=0; i<10; i++)
		{
			String selectQuery = 
					"SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE "
					+ "attr0 >= 2 AND attr0 <= 1500 AND "
					+ "attr2 >= 2 AND attr2 <= 1500 AND "
					+ "attr5 >= 2 AND attr5 <= 1500";
				
			JSONArray replyArray = new JSONArray();
			long expiryTime = 300000; // 5 min
			
			int numRep = csClient.sendSearchQuerySecure
					(selectQuery, replyArray, expiryTime, aclMemberGuids.get(i));
			
			// nobody except frm user allowed to read
			//assertTrue(replyArray.get(0).equals(userGUID.getGuid()));
			assertEquals(0, numRep);
			assertEquals(0, replyArray.length());
		}
		
		// check with user
		String selectQuery = 
				"SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE "
				+ "attr0 >= 2 AND attr0 <= 1500 AND "
				+ "attr2 >= 2 AND attr2 <= 1500 AND "
				+ "attr5 >= 2 AND attr5 <= 1500";
			
		JSONArray replyArray = new JSONArray();
		long expiryTime = 300000; // 5 min
		
		int numRep = csClient.sendSearchQuerySecure
				(selectQuery, replyArray, expiryTime, userGUID);
		
		// nobody except frm user allowed to read
		assertTrue(userGUID.getGuid().compareToIgnoreCase(replyArray.getString(0)) == 0  );
		assertEquals(1, numRep);
		assertEquals(1, replyArray.length());
	}
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception 
	{
//		if (System.getProperty("startServer") != null
//	            && System.getProperty("startServer").equals("true")) 
//		{
//			ArrayList<String> output = RunServer.command(
//	              "scripts/3nodeslocal/shutdown.sh", ".");
//	      if (output != null) {
//	        for (String line : output) {
//	          System.out.println(line);
//	        }
//	      } else {
//	        System.out.println("SHUTDOWN SERVER COMMAND FAILED!");
//	      }
//	    }
//	    if (client != null) {
//	      client.close();
//	    }
	 }
	
	private static String getGUID(String stringToHash)
	{
	   MessageDigest md=null;
	   try
	   {
		   md = MessageDigest.getInstance("SHA-256");
	   } catch (NoSuchAlgorithmException e)
	   {
		   e.printStackTrace();
	   }
       
	   md.update(stringToHash.getBytes());
 
       byte byteData[] = md.digest();
 
       //convert the byte to hex format method 1
       StringBuffer sb = new StringBuffer();
       for (int i = 0; i < byteData.length; i++) 
       {
       		sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
       }
       
       String returnGUID = sb.toString();
       return returnGUID.substring(0, 40);
	}
	
	private static JSONObject getARandomAttrValSet() throws JSONException
	{
		JSONObject valJSON = new JSONObject();
		Random rand = new Random();
		
		for(int i=0; i < 6; i++)
		{
			// 1 is the default value so that is ignored
			double randVal = 2.0+ 1498.0*rand.nextDouble();
			valJSON.put("attr"+i, randVal);
		}
		return valJSON;
	}
	
	private static void startFourNodeSetup() throws Exception
	{
		String[] args = new String[4];
		for(int i=0; i<4; i++)
		{
			args[0] = "-id";
			args[1] = i+"";
			args[2] = "-csConfDir";
			args[3] = ContextServiceConfig.configFileDirectory;
			
			StartContextServiceNode.main(args);
		}
	}
	
	private static GuidEntry getAGUIDEntry(String guidAlias) 
											throws NoSuchAlgorithmException
	{
		KeyPair kp = kpg.genKeyPair();
		PublicKey publicKey = kp.getPublic();
		PrivateKey privateKey = kp.getPrivate();
		byte[] publicKeyByteArray = publicKey.getEncoded();
		
		String guid = Utils.convertPublicKeyToGUIDString(publicKeyByteArray);
		GuidEntry guidEntry = new GuidEntry(guidAlias, guid, publicKey, privateKey);
		
		return guidEntry;
	}
}