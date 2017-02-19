package edu.umass.cs.contextservice.test;

import java.util.Properties;


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
		
//		InetSocketAddress address 
//			= new InetSocketAddress("127.0.0.1", GNSClientConfig.LNS_PORT);
//		GNSClientCommands gnsClient = new GNSClientCommands();
//		
//		GuidEntry masterGuid = GuidUtils.lookupOrCreateAccountGuid(gnsClient,
//                "gnsumass@gmail.com", "password", true);
//		
//		System.out.println("masterGuid "+masterGuid.getGuid());
	}
}