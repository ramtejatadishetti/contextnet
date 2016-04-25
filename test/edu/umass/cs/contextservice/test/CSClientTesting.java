package edu.umass.cs.contextservice.test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.json.JSONObject;

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClientExtended;
import edu.umass.cs.gnsclient.client.util.GuidUtils;

public class CSClientTesting 
{
	public static void main(String[] args) throws Exception
	{
		Properties props = System.getProperties();
		props.setProperty("gigapaxosConfig", "conf/gnsClientConf/gigapaxos.client.local.properties");
		props.setProperty("javax.net.ssl.trustStorePassword", "qwerty");
		props.setProperty("javax.net.ssl.trustStore", "conf/gnsClientConf/trustStore/node100.jks");
		props.setProperty("javax.net.ssl.keyStorePassword", "qwerty");
		props.setProperty("javax.net.ssl.keyStore", "conf/gnsClientConf/keyStore/node100.jks");	
		
		
		InetSocketAddress address 
			= new InetSocketAddress("127.0.0.1", GNSClientConfig.LNS_PORT);
		UniversalTcpClientExtended gnsClient = new GNSClient(null, address, true);
		
		GuidEntry masterGuid = GuidUtils.lookupOrCreateAccountGuid(gnsClient,
                "gnsumass@gmail.com", "password", true);
		
		System.out.println("masterGuid "+masterGuid.getGuid());
		
		
		ContextServiceClient<Integer> csClient = new
					ContextServiceClient<Integer>( "127.0.0.1", 8000, 
									"127.0.0.1", GNSClientConfig.LNS_PORT,
									ContextServiceClient.SUBSPACE_BASED_CS_TRANSFORM );
		
		
		HashMap<String, List<ACLEntry>> aclMap 
									= new HashMap<String, List<ACLEntry>>();
		
		List<AnonymizedIDEntry> anonymizedIDList = 
										csClient.computeAnonymizedIDs(aclMap);
		
		JSONObject gnsAttrValuePairs = new JSONObject();
		gnsAttrValuePairs.put("attr0", 10.0);
		
		gnsAttrValuePairs.put("attr3", 15.0);
		
		csClient.sendUpdateSecure( masterGuid.getGuid() , masterGuid , 
				gnsAttrValuePairs , -1 , true , 
				aclMap , anonymizedIDList );
		
		JSONObject getRep 
			= csClient.sendGetRequestSecure(masterGuid.getGuid(), masterGuid);
		
		System.out.println( "getRep "+getRep );
	}
}