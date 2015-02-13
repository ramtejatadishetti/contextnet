package edu.umass.cs.contextservice.test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.KeyPair;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.CSNodeConfig;
import edu.umass.cs.contextservice.ContextServiceDemultiplexer;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gns.client.GuidEntry;
import edu.umass.cs.gns.client.UniversalTcpClient;
import edu.umass.cs.gns.client.util.KeyPairUtils;
import edu.umass.cs.gns.nio.JSONMessageExtractor;
import edu.umass.cs.gns.nio.JSONNIOTransport;

public class ReconcileTimeExp<NodeIDType>
{
	public static String csServerName 					= "ananas.cs.umass.edu";
	public static int csPort 							= 5000;
	
	
	private static final String defaultGns = KeyPairUtils.getDefaultGnsFromPreferences();
	private static final UniversalTcpClient gnsClient 
				= new UniversalTcpClient(defaultGns.split(":")[0], Integer.parseInt(defaultGns.split(":")[1]));
	
	private static final GuidEntry myGuidEntry = KeyPairUtils.getDefaultGuidEntryFromPreferences(defaultGns);
	
	private NodeIDType myID;
	private CSNodeConfig<NodeIDType> csNodeConfig;
	private JSONNIOTransport<NodeIDType> niot;
	
	
	
	
	public ReconcileTimeExp(NodeIDType id) throws IOException
	{
		myID = id;
		
		csNodeConfig = new CSNodeConfig<NodeIDType>();
		
		csNodeConfig.add(myID, new InetSocketAddress(Utils.getActiveInterfaceInetAddresses().get(0), 9189));
		
		JSONMessageExtractor worker = new JSONMessageExtractor(new ContextServiceDemultiplexer());
		
		niot = new JSONNIOTransport<NodeIDType>(myID, csNodeConfig, worker);
        new Thread(niot).start();
        
        //udpserver_socket = new DatagramSocket(5000);
	}
	
	
	public void sendQueryToContextService(String query
			/*,InterfaceJSONNIOTransport<NodeIDType> nioObject*/)
	{
		try
		{
			QueryMsgFromUser<NodeIDType> qmesgU = new QueryMsgFromUser<NodeIDType>(myID, query);
			
	        niot.sendToAddress(new InetSocketAddress(InetAddress.getByName(csServerName), csPort)
									, qmesgU.toJSONObject());
			
			System.out.println("Query sent "+query+" to "+csServerName+" port "+csPort);
		} catch (JSONException e)
		{
			e.printStackTrace();
		} catch (UnknownHostException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * creates the group for a query, adding 
	 * query as the alias.
	 * Initially the group is empty but 
	 * GUIDs are added as soon as query processing is 
	 * complete.
	 * @return groupGUID if successful otherwise empty string
	 */
	public static void createGUIDsAndAttributes()
	{
		try
		{
			String GUIDAliasprefix				= "reconcileExp";
			/*String defaultGns = KeyPairUtils.getDefaultGnsFromPreferences();
			System.out.println(" createQueryGroup defaultGns "+defaultGns);
			
			String[] parsed = defaultGns.split(":");
			String gnsHost = parsed[0];
			int gnsPort = Integer.parseInt(parsed[1]);
			
			GuidEntry myGuidEntry = KeyPairUtils.getDefaultGuidEntryFromPreferences(defaultGns);
			
			UniversalTcpClient gnsClient = new UniversalTcpClient(gnsHost, gnsPort);*/
			
		    //GuidEntry groupGuid = KeyPairUtils.getGuidEntryFromPreferences(defaultGns, queryHash);
		    
		    /*
		     * Take a lock on the GNS connection object to prevent concurrent queries to
		     * the GNS on the same connection as the library is not thread-safe from
		     * that standpoint.
		     */
		    synchronized (gnsClient)
		    {
		    	for(int i=0;i<100;i++)
		    	{
		    		String currAlias = GUIDAliasprefix +i;
		    		String currGUIDString = "";
		    		boolean guidFound = false;
			    	try
			    	{
			    		// if lookup succeeds then there is a group that exists,
			    		// just reset it. If lookup doesn't succeed then 
			    		// create the group.
			    		currGUIDString = gnsClient.lookupGuid(currAlias);
			    		guidFound = true;
			    	} catch(Exception ex)
			    	{
			    		// lookup failed, create the group.
			    		guidFound = false;
			    	}
			    	
			    	//create the group
			    	if(!guidFound)
			    	{
			    		System.out.println
			    		("No group exisits " + currAlias +". Generating new GUID and keys");
			    		
			    		// Create a new GUID
			    		GuidEntry guidEntry = gnsClient.guidCreate(myGuidEntry, currAlias);
			    		
			    		// save keys in the preference
			    		System.out.println("saving keys to local");
			    		KeyPairUtils.saveKeyPairToPreferences(KeyPairUtils.getDefaultGnsFromPreferences(), 
			    				guidEntry.getEntityName() , guidEntry.getGuid(), 
			    			  new KeyPair(guidEntry.getPublicKey(), guidEntry.getPrivateKey()));
			
			    		// storing alias in gns record, need it to find it when we have GUID
			    		// from group members
			    		
			    		//gnsClient.addAlias(groupGuid, queryHash);
			    		currGUIDString = guidEntry.getGuid();
			    		
			    		// set the attributes for the group
			    		gnsClient.fieldReplaceOrCreateList
			    		(currGUIDString, "contextATT0", new JSONArray("100"), guidEntry);
			    	} else // reset the group
			    	{
			    		System.out.println("group already exists, just setting the attributes "+currGUIDString);
			    		//FIXME: need one command to reset the group.
			    		//JSONArray jsonMem = gnsClient.groupGetMembers(groupGUIDString, myGuidEntry);
			    		//if(jsonMem.length() > 0)
			    		//{
			    		//	log.info("number of members already in group "+jsonMem.length());
			    		//	gnsClient.groupRemoveGuids(groupGUIDString, jsonMem, myGuidEntry);
			    		//}
			    		GuidEntry guidEntry = KeyPairUtils.getGuidEntryFromPreferences(KeyPairUtils.getDefaultGnsFromPreferences(),
			    				currAlias);
			    		gnsClient.fieldReplaceOrCreateList
			    		(currGUIDString, "contextATT0", new JSONArray("100"), guidEntry);
			    	}
		    	}
		    }
		} catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	/**
	 * takes queries input from user
	 */
	public static void createGroup(ReconcileTimeExp<Integer> basicObj)
	{
	      String query = "50 <= contextATT0 <= 150";
	      
		  basicObj.sendQueryToContextService(query);
	}
	
	/*public static void createGroup(String groupQuery)
	{
	}*/
	
	public static void main(String[] args) throws IOException
	{
		if(args.length >= 2)
		{
			csServerName = args[0];
			csPort = Integer.parseInt(args[1]);
		}
		
		Integer id = 0;
		ReconcileTimeExp<Integer> basicObj = new ReconcileTimeExp<Integer>(id);
		
		for(int i=0;i<10;i++)
		{
			//simpleQueryWorkload(basicObj);
			//largeQueryWorkload(basicObj);
			//queryEachAttributeWorkload(basicObj);
		}
	}
	
	
	/*public static void startUDPServer() throws Exception
    {
		byte[] receive_data = new byte[1024];
		byte[] send_data = new byte[1024];
		
		int recv_port;
		System.out.println ("UDPServer Waiting for client on port 5000");
		
		while(true)
		{
			DatagramPacket receive_packet = new DatagramPacket(receive_data,
                                            receive_data.length);
			server_socket.receive(receive_packet);
			String data = new String(receive_packet.getData(),0                                          ,0
                                         ,receive_packet.getLength());
            InetAddress IPAddress = receive_packet.getAddress();          
            recv_port = receive_packet.getPort();
            
            if (data.equals("q") || data.equals("Q"))
            {
            	break;
            }
            else
            {
            	System.out.println("( " + IPAddress + " , " + recv_port 
                			+ " ) said :" + data );
            }
       }
    }*/
    
}