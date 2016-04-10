package edu.umass.cs.contextservice.test;

import java.net.InetSocketAddress;
import java.util.Properties;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClientExtended;
import edu.umass.cs.gnsclient.client.util.GuidUtils;

public class GNSClientTesting 
{
	public static void main(String[] args) throws Exception
	{
//		-DgigapaxosConfig=conf/gnsClientConf/gigapaxos.client.local.properties 
//		-Djavax.net.ssl.trustStorePassword=qwerty
//		-Djavax.net.ssl.trustStore=conf/gnsClientConf/trustStore/node100.jks  
//		-Djavax.net.ssl.keyStorePassword=qwerty  
//		-Djavax.net.ssl.keyStore=conf/gnsClientConf/keyStore/node100.jks
		
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
	}
}