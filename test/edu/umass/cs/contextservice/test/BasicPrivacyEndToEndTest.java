package edu.umass.cs.contextservice.test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;

/**
 * 
 * Start FourNodeCSSetup before starting BasicPrivacyEndToEndTest
 * @author adipc
 */
public class BasicPrivacyEndToEndTest 
{
	// testing secure client code
	// TODO: test the scheme with the example given in the draft.
	public static void main(String[] args) 
			throws Exception
	{
		// testing based on the example in the draft.
		// more testing of each method in secure interface.
		// test with the example in the draft.
		//GUIDEntryStoringClass myGUIDInfo, JSONArray ACLArray
		//String guid = GuidUtils.createGuidFromPublicKey(keyPair.getPublic().getEncoded());
//			Properties props = System.getProperties();
//			props.setProperty("gigapaxosConfig", "conf/gnsClientConf/gigapaxos.client.local.properties");
//			props.setProperty("javax.net.ssl.trustStorePassword", "qwerty");
//			props.setProperty("javax.net.ssl.trustStore", "conf/gnsClientConf/trustStore/node100.jks");
//			props.setProperty("javax.net.ssl.keyStorePassword", "qwerty");
//			props.setProperty("javax.net.ssl.keyStore", "conf/gnsClientConf/keyStore/node100.jks");
//			
//			InetSocketAddress address 
//				= new InetSocketAddress("127.0.0.1", GNSClientConfig.LNS_PORT);
//			UniversalTcpClientExtended gnsClient = new GNSClient(null, address, true);
//	
//			GuidEntry masterGuid = GuidUtils.lookupOrCreateAccountGuid(gnsClient,
//	            "ayadav@cs.umass.edu", "password", true);
	
		KeyPairGenerator kpg;
		kpg = KeyPairGenerator.getInstance("RSA");
		KeyPair kp0 = kpg.genKeyPair();
		PublicKey publicKey0 = kp0.getPublic();
		PrivateKey privateKey0 = kp0.getPrivate();
		byte[] publicKeyByteArray0 = publicKey0.getEncoded();
		byte[] privateKeyByteArray0 = privateKey0.getEncoded();
		
		String guid0 = GuidUtils.createGuidFromPublicKey(publicKeyByteArray0);
		GuidEntry myGUID = new GuidEntry("Guid0", guid0, publicKey0, privateKey0);
		
//			PublicKey publicKey0 = masterGuid.getPublicKey();
//			PrivateKey privateKey0 = masterGuid.getPrivateKey();
//			byte[] publicKeyByteArray0 = publicKey0.getEncoded();
//			byte[] privateKeyByteArray0 = privateKey0.getEncoded();
//			
//			String guid0 = GuidUtils.createGuidFromPublicKey(publicKeyByteArray0);
//			GuidEntry myGUID = masterGuid;	
		
		Vector<GuidEntry> guidsVector = new Vector<GuidEntry>();
		
		
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
		
		HashMap<String, List<ACLEntry>> aclMap = new HashMap<String, List<ACLEntry>>();
		
		List<ACLEntry> acl0 = new LinkedList<ACLEntry>();
		
		acl0.add(new ACLEntry(guidsVector.get(1).getGuid(), guidsVector.get(1).getPublicKey()));
		acl0.add(new ACLEntry(guidsVector.get(2).getGuid(), guidsVector.get(2).getPublicKey()));
		acl0.add(new ACLEntry(guidsVector.get(3).getGuid(), guidsVector.get(3).getPublicKey()));
		aclMap.put("attr0", acl0);
		
		
		List<ACLEntry> acl1 = new LinkedList<ACLEntry>();
		acl1.add(new ACLEntry(guidsVector.get(4).getGuid(), guidsVector.get(4).getPublicKey()));
		acl1.add(new ACLEntry(guidsVector.get(5).getGuid(), guidsVector.get(5).getPublicKey()));
		acl1.add(new ACLEntry(guidsVector.get(3).getGuid(), guidsVector.get(3).getPublicKey()));
		aclMap.put("attr1", acl1);
		
		
		List<ACLEntry> acl2 = new LinkedList<ACLEntry>();
		acl2.add(new ACLEntry(guidsVector.get(1).getGuid(), guidsVector.get(1).getPublicKey()));
		acl2.add(new ACLEntry(guidsVector.get(2).getGuid(), guidsVector.get(2).getPublicKey()));
		aclMap.put("attr2", acl2);
		
		
		List<ACLEntry> acl3 = new LinkedList<ACLEntry>();
		acl3.add(new ACLEntry(guidsVector.get(1).getGuid(), guidsVector.get(1).getPublicKey()));
		acl3.add(new ACLEntry(guidsVector.get(2).getGuid(), guidsVector.get(2).getPublicKey()));
		acl3.add(new ACLEntry(guidsVector.get(3).getGuid(), guidsVector.get(3).getPublicKey()));
		aclMap.put("attr3", acl3);
		
		
		List<ACLEntry> acl4 = new LinkedList<ACLEntry>();
		acl4.add(new ACLEntry(guidsVector.get(6).getGuid(), guidsVector.get(6).getPublicKey()));
		acl4.add(new ACLEntry(guidsVector.get(7).getGuid(), guidsVector.get(7).getPublicKey()));
		aclMap.put("attr4", acl4);
		
		
		List<ACLEntry> acl5 = new LinkedList<ACLEntry>();
		acl5.add(new ACLEntry(guidsVector.get(4).getGuid(), guidsVector.get(4).getPublicKey()));
		acl5.add(new ACLEntry(guidsVector.get(5).getGuid(), guidsVector.get(5).getPublicKey()));
		acl5.add(new ACLEntry(guidsVector.get(1).getGuid(), guidsVector.get(1).getPublicKey()));
		
		aclMap.put("attr5", acl5);
		
		
		ContextServiceClient<Integer> csClient = new ContextServiceClient<Integer>("127.0.0.1", 8000);
		
		List<AnonymizedIDEntry> anonymizedIdList = csClient.computeAnonymizedIDs(aclMap);
		JSONObject attrValPair = new JSONObject();
		attrValPair.put("attr0", 10+"");
		
		attrValPair.put("attr1", 15+"");
		
		csClient.sendUpdateSecure(guid0, myGUID, attrValPair, -1, true, aclMap, anonymizedIdList);
		
		Thread.sleep(2000);
		
		String searchQuery = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE attr0 >= 5 AND attr0 <= 15";
		JSONArray replyArray = new JSONArray();
		GuidEntry queryingGuid = guidsVector.get(3);
		csClient.sendSearchQuerySecure(searchQuery, replyArray, 300000, queryingGuid);
		
		System.out.println("Query for attr1 querying GUID "+ queryingGuid.getGuid()+
				" Real GUID "+guid0+" reply Arr "+replyArray);
		
		
		searchQuery = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE attr0 >= 5 AND attr0 <= 15"
				+ " AND attr1 >= 10 AND attr1 <= 20";
		replyArray = new JSONArray();
		queryingGuid = guidsVector.get(1);
		csClient.sendSearchQuerySecure(searchQuery, replyArray, 300000, queryingGuid);
		
		System.out.println("Query for att0 and attr3 querying GUID "+ queryingGuid.getGuid()+
				" Real GUID "+guid0+" reply Arr "+replyArray);
		
//			queryingGuid = guidsVector.get(1);
//			JSONObject getObj = csClient.sendGetRequestSecure(guid0, queryingGuid);
//			System.out.println("recvd Obj "+getObj);
	}
}