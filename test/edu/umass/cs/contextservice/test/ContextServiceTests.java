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
import edu.umass.cs.contextservice.config.ContextServiceConfig.PrivacySchemes;
import edu.umass.cs.contextservice.nodeApp.StartContextServiceNode;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ContextServiceTests 
{
	private static KeyPairGenerator kpg;
	private static ContextServiceClient csClient 	= null;
	
	private static String memberAliasPrefix 				= "clientGUID";
	
	private static String csNodeIp 							= "127.0.0.1";
	private static int csPort 								= 8000;
	
	private static PrivacySchemes privacyScheme				= PrivacySchemes.SUBSPACE_PRIVACY;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		kpg = KeyPairGenerator.getInstance("RSA");
		// context service config to use.
		ContextServiceConfig.configFileDirectory 
			= "conf/testConf/contextServiceConf";
		
		// setting all config parameters for the test.
		ContextServiceConfig.sendFullRepliesToClient 	= true;
		ContextServiceConfig.sendFullRepliesWithinCS 	= true;
		ContextServiceConfig.TRIGGER_ENABLED 			= true;
		ContextServiceConfig.UniqueGroupGUIDEnabled     = true;
		ContextServiceConfig.PRIVACY_ENABLED			= true;
		ContextServiceClient.EXPERIMENT_MODE            = false;
		
		
		// start context service.
		startFourNodeSetup();
		
		
		// make a client connection
		csClient = new ContextServiceClient(csNodeIp, csPort, false,
				privacyScheme);
		
		System.out.println("ContextServiceClient connected using privacy scheme ordinal "
							+privacyScheme.ordinal());
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
		
		assertEquals(0, numRep);
		assertEquals(0, replyArray.length());
	}
	
	@Test
	public void test_2_Input100GUIDs() throws JSONException 
	{
		// these tests require full search replies to be sent.
		assert( ContextServiceConfig.sendFullRepliesToClient );
		assert( ContextServiceConfig.sendFullRepliesWithinCS );
		
		for(int i=0; i<100; i++)
		{
			String realAlias = memberAliasPrefix+i;
			String myGUID = getGUID(realAlias);
			JSONObject attrValJSON = getARandomAttrValSet();
			csClient.sendUpdate(myGUID, null, attrValJSON, -1);
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
	public void test_4_noGNSprivacyTest() 
			throws JSONException, NoSuchAlgorithmException, EncryptionException
	{
		// these tests require full search replies to be sent.
		assert( ContextServiceConfig.sendFullRepliesToClient );
		assert( ContextServiceConfig.sendFullRepliesWithinCS );
		
		assert(ContextServiceConfig.PRIVACY_ENABLED);
		
		assert(ContextServiceConfig.DECRYPTIONS_ON_SEARCH_REPLY_ENABLED);
		// if privacy not enabled then just return.
		
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
		
		for( int i=0; i<6; i++ )
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
					//System.out.println("attrName "+attrName+" GUID "+
					//		aclMemberGuids.get(j).getGuid());
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
					
					//System.out.println("attrName "+attrName+" GUID "+
					//		aclMemberGuids.get(j).getGuid());
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
					= csClient.computeAnonymizedIDs(userGUID, aclMap, false);
		
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
					-1, aclMap, anonymizedIDsList);
		
		
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
				if (ContextServiceConfig.DECRYPTIONS_ON_SEARCH_REPLY_ENABLED)
				{
					//assertEquals(1, numRep);
					//assertEquals(1, replyArray.length());
					//assertTrue(userGUID.getGuid().compareToIgnoreCase
					//		(replyArray.getString(0)) == 0  );
					
					boolean found = checkAGuidInReplyArray( userGUID.getGuid(), 
							replyArray );
					
					assert(found);
				}
				else
				{
					// as multiple anonymized IDs may be returned.
					assert(numRep > 0);
					assert(replyArray.length() > 0);
				}
			}
			else
			{
				//assertTrue(replyArray.get(0).equals(userGUID.getGuid()));
				if ( ContextServiceConfig.DECRYPTIONS_ON_SEARCH_REPLY_ENABLED )
				{
					//assertEquals(0, numRep);
					//assertEquals(0, replyArray.length());
					boolean found = checkAGuidInReplyArray( userGUID.getGuid(), 
							replyArray );
					
					assert(!found);
				}
				else
				{
					assert(numRep > 0);
					assert(replyArray.length() > 0);
				}
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
			if( ContextServiceConfig.DECRYPTIONS_ON_SEARCH_REPLY_ENABLED )
			{
				//assertEquals(0, numRep);
				//assertEquals(0, replyArray.length());
				boolean found = checkAGuidInReplyArray( userGUID.getGuid(), 
						replyArray );
				
				assert(!found);
			}
			else
			{
				// satisfying anonymzied IDs will be returned.
				// so they should be greater than zero.
				assert(numRep > 0);
				assert(replyArray.length() > 0);
			}
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
		
		if( ContextServiceConfig.DECRYPTIONS_ON_SEARCH_REPLY_ENABLED )
		{
			// nobody except from user allowed to read
			//assertTrue
			//( userGUID.getGuid().compareToIgnoreCase(replyArray.getString(0)) == 0  );
			//assertEquals(1, numRep);
			//assertEquals(1, replyArray.length());
			
			boolean found = checkAGuidInReplyArray( userGUID.getGuid(), 
					replyArray );
			
			assert(found);
		}
		else
		{
			assert( numRep > 0 );
			assert( replyArray.length() > 0 );
		}
	}
	
	
	@Test
	public void test_3_GNSprivacyTest() 
			throws Exception
	{
		// these tests require full search replies to be sent.
		assert( ContextServiceConfig.sendFullRepliesToClient );
		assert( ContextServiceConfig.sendFullRepliesWithinCS );
		
		assert(ContextServiceConfig.PRIVACY_ENABLED);
		
		assert(ContextServiceConfig.DECRYPTIONS_ON_SEARCH_REPLY_ENABLED);
		// if privacy not enabled then just return.
		
		ContextServiceClient csClient 
				= new ContextServiceClient(csNodeIp, csPort, true,
						privacyScheme);
		
		
		GNSClient gnsClient = csClient.getGNSClient();
		assert(gnsClient != null);
		GuidEntry userGUID = GuidUtils.lookupOrCreateAccountGuid
									(gnsClient, "userGuid1@gmail.com", "password");
		
		gnsClient.execute( GNSCommand.fieldReplaceOrCreateList
				(userGUID, 
				ContextServiceClient.SYMMETRIC_KEY_EXCHANGE_FIELD_NAME, 
				new JSONArray()));
		
		System.out.println("User GUID "+ userGUID.getGuid());
		
		Vector<GuidEntry> aclMemberGuids = new Vector<GuidEntry>();
		
		for( int i=0; i<10; i++ )
		{
			String guidAlias = "acl4Member"+i+"@gmail.com";
			
			System.out.println("Creating "+guidAlias);
			
			GuidEntry aclMem = null;
			try
			{
				aclMem = GuidUtils.lookupOrCreateAccountGuid
						(gnsClient, guidAlias, "password");
			} catch(Exception ex)
			{
				ex.printStackTrace();
				continue;
			}
			
			
			System.out.println("GUID for alias "+guidAlias+" "+aclMem.getGuid());
			// clear any old keys in the field.
			gnsClient.execute( GNSCommand.fieldReplaceOrCreateList
					(aclMem, 
					ContextServiceClient.SYMMETRIC_KEY_EXCHANGE_FIELD_NAME, 
					new JSONArray()));
			
			System.out.println("Clear for alias "+guidAlias+" complete");
			
			// any GUID can append symmetric key information here.
			gnsClient.execute( GNSCommand.aclAdd(AclAccessType.WRITE_WHITELIST, aclMem, 
					ContextServiceClient.SYMMETRIC_KEY_EXCHANGE_FIELD_NAME, GNSCommandProtocol.ALL_GUIDS) );
			
			System.out.println("ACL write whitelist set for "+guidAlias+" complete");
			//GuidEntry aclMem = getAGUIDEntry("aclMember"+i);
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
					= csClient.computeAnonymizedIDs(userGUID, aclMap, true);
		
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
					-1, aclMap, anonymizedIDsList);
		
		
		for(int i=0; i<10; i++)
		{
			String selectQuery = 
					"SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE "
					+ "attr0 >= 2 AND attr0 <= 1500 AND "
					+ "attr2 >= 2 AND attr2 <= 1500 AND "
					+ "attr4 >= 2 AND attr4 <= 1500";
				
			JSONArray replyArray = new JSONArray();
			long expiryTime = 300000; // 5 min
			
			
			HashMap<String, byte[]> anonymizedIDToSecretKeyMap 
						= csClient.getAnonymizedIDToSymmetricKeyMapFromGNS(aclMemberGuids.get(i));
			
			System.out.println(i+" anonymizedIDToSecretKeyMap size "
										+anonymizedIDToSecretKeyMap.size());	
			
			int numRep = csClient.sendSearchQuerySecure
					(selectQuery, replyArray, expiryTime, anonymizedIDToSecretKeyMap );
			
			
			// querier is allowed to read
			if(i<5)
			{
				if ( ContextServiceConfig.DECRYPTIONS_ON_SEARCH_REPLY_ENABLED )
				{
					boolean found = checkAGuidInReplyArray( userGUID.getGuid(), 
							replyArray );
					
					assert(found);
				}
				else
				{
					// as multiple anonymized IDs may be returned.
					assert(numRep >= 1);
					assert(replyArray.length() >= 1);
				}
			}
			else
			{
				//assertTrue(replyArray.get(0).equals(userGUID.getGuid()));
				if ( ContextServiceConfig.DECRYPTIONS_ON_SEARCH_REPLY_ENABLED )
				{
					boolean found = checkAGuidInReplyArray( userGUID.getGuid(), 
							replyArray );
					
					assert(!found);
				}
				else
				{
					assert(numRep > 0);
					assert(replyArray.length() > 0);
				}
			}
		}
		
		// across two different sets
		// nobody except from userGUID should be allowed to read.
		for( int i=0; i<10; i++ )
		{
			String selectQuery = 
					"SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE "
					+ "attr0 >= 2 AND attr0 <= 1500 AND "
					+ "attr2 >= 2 AND attr2 <= 1500 AND "
					+ "attr5 >= 2 AND attr5 <= 1500";
			
			JSONArray replyArray = new JSONArray();
			long expiryTime = 300000; // 5 min
			
			HashMap<String, byte[]> anonymizedIDToSecretKeyMap 
						= csClient.getAnonymizedIDToSymmetricKeyMapFromGNS(aclMemberGuids.get(i));
			
			int numRep = csClient.sendSearchQuerySecure
					(selectQuery, replyArray, expiryTime, anonymizedIDToSecretKeyMap);
			
			// nobody except frm user allowed to read
			//assertTrue(replyArray.get(0).equals(userGUID.getGuid()));
			if( ContextServiceConfig.DECRYPTIONS_ON_SEARCH_REPLY_ENABLED )
			{
				//assertEquals(0, numRep);
				//assertEquals(0, replyArray.length());
				boolean found = checkAGuidInReplyArray( userGUID.getGuid(), 
						replyArray );
				
				assert(!found);
			}
			else
			{
				// satisfying anonymzied IDs will be returned.
				// so they should be greater than zero.
				assert(numRep > 0);
				assert(replyArray.length() > 0);
			}
		}
		
		// check with user
		String selectQuery = 
				"SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE "
				+ "attr0 >= 2 AND attr0 <= 1500 AND "
				+ "attr2 >= 2 AND attr2 <= 1500 AND "
				+ "attr5 >= 2 AND attr5 <= 1500";
			
		JSONArray replyArray = new JSONArray();
		long expiryTime = 300000; // 5 min
		
		HashMap<String, byte[]> anonymizedIDToSecretKeyMap 
				= csClient.getAnonymizedIDToSymmetricKeyMapFromGNS(userGUID);
		
		int numRep = csClient.sendSearchQuerySecure
				(selectQuery, replyArray, expiryTime, anonymizedIDToSecretKeyMap);
		
		if( ContextServiceConfig.DECRYPTIONS_ON_SEARCH_REPLY_ENABLED )
		{
			// nobody except from user allowed to read
			//assertTrue
			//( userGUID.getGuid().compareToIgnoreCase(replyArray.getString(0)) == 0  );
			//assertEquals(1, numRep);
			//assertEquals(1, replyArray.length());
			
			boolean found = checkAGuidInReplyArray( userGUID.getGuid(), 
					replyArray );
			
			assert(found);
		}
		else
		{
			assert( numRep > 0 );
			assert( replyArray.length() > 0 );
		}
	}
	
	@Test
	public void test_5_TriggerTest() throws JSONException
	{
		//FIXME: need to add a circular query trigger test
		// these tests require full search replies to be sent.
		assert( ContextServiceConfig.sendFullRepliesToClient );
		assert( ContextServiceConfig.sendFullRepliesWithinCS );
		
		//Random rand = new Random();
		String realAlias = memberAliasPrefix+10000;
		String myGUID = getGUID(realAlias);
		JSONObject attrValJSON = new JSONObject();
		attrValJSON.put("attr0", 500);
		attrValJSON.put("attr1", 500);
		attrValJSON.put("attr2", 500);
		attrValJSON.put("attr3", 500);
		attrValJSON.put("attr4", 500);
		attrValJSON.put("attr5", 500);
		
		System.out.println("Inserting "+myGUID);
		csClient.sendUpdate(myGUID, null, attrValJSON, -1);
		
		String selectQuery = 
			"SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE "
			+ "attr0 >= 2 AND attr0 <= 750 AND "
			+ "attr1 >= 2 AND attr1 <= 750 AND "
			+ "attr2 >= 2 AND attr2 <= 750 AND "
			+ "attr3 >= 2 AND attr3 <= 750 AND "
			//+ "attr4 >= 1400 AND attr4 <= 600 AND "
			+ "attr4 >= 2 AND attr4 <= 750 AND "
			+ "attr5 >= 2 AND attr5 <= 750";
		
		
		JSONArray replyArray = new JSONArray();
		long expiryTime = 300000; // 5 min
		int numRep = csClient.sendSearchQuery(selectQuery, replyArray, expiryTime);
		
		assert(numRep >=1);
		
		String guidToUpdate = replyArray.getString(0);
		System.out.println("Guid to update "+guidToUpdate);
		JSONObject updatedJSON = new JSONObject();
		
		updatedJSON.put("attr4", 1000);
		
		csClient.sendUpdate(guidToUpdate, null, updatedJSON, -1);
		
		JSONArray triggerArray = new JSONArray();
		csClient.getQueryUpdateTriggers(triggerArray);
		System.out.println("triggers recvd "+triggerArray);
		
		assert(triggerArray.length() >= 1);
//		assertEquals(100, numRep);
//		assertEquals(100, replyArray.length());
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
		for( int i=0; i<4; i++ )
		{
			args[0] = "-id";
			args[1] = i+"";
			args[2] = "-csConfDir";
			args[3] = ContextServiceConfig.configFileDirectory;
			
			StartContextServiceNode.main(args);
		}
	}
	
	private static GuidEntry getAGUIDEntry(String guidAlias) 
											throws NoSuchAlgorithmException, EncryptionException
	{
		KeyPair kp = kpg.genKeyPair();
		PublicKey publicKey = kp.getPublic();
		PrivateKey privateKey = kp.getPrivate();
		byte[] publicKeyByteArray = publicKey.getEncoded();
		
		String guid = Utils.convertPublicKeyToGUIDString(publicKeyByteArray);
		GuidEntry guidEntry = new GuidEntry(guidAlias, guid, publicKey, privateKey);
		
		return guidEntry;
	}
	
	private static boolean checkAGuidInReplyArray( String guidToCheck, 
													JSONArray replyArray )
	{
		boolean found = false;
		
		for( int j=0; j<replyArray.length(); j++ )
		{
			try 
			{
				if( guidToCheck.compareToIgnoreCase
						(replyArray.getString(j)) == 0 )
				{
					found = true;
					break;
				}
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
		return found;
	}
}