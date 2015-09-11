package edu.umass.cs.contextservice.examples.basic;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import edu.umass.cs.contextservice.CSNodeConfig;
import edu.umass.cs.contextservice.ContextServiceNode;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.nio.InterfaceNodeConfig;

/**
 * Planetlab context service example with 10 nodes, with simple
 * conjunction query.
 * @author adipc
 */
public class PlanetLabContextServiceExample extends ContextServiceNode<Integer>
{
	public static final int CONTEXTNET									= 1;
	public static final int REPLICATE_ALL								= 2;
	public static final int QUERY_ALL									= 3;
	public static final int MERCURY										= 4;
	public static final int HYPERDEX									= 5;
	public static final int MERCURYNEW									= 6;
	public static final int MERCURYCONSISTENT							= 7;
	
	
	private static CSNodeConfig<Integer> csNodeConfig					= null;
	
	//private static DatagramSocket server_socket;
	
	private static PlanetLabContextServiceExample[] nodes				= null;
	
	public static final String configFileName							= "contextServiceNodeSetup.txt";
	
	public PlanetLabContextServiceExample(Integer id, InterfaceNodeConfig<Integer> nc)
			throws IOException
	{
		super(id, nc);
	}
	
	public static void main(String[] args) throws NumberFormatException, UnknownHostException, IOException
	{
		Integer myID = Integer.parseInt(args[0]);
		int schemeType = Integer.parseInt(args[1]);
		int numAttr = Integer.parseInt(args[2]);
		ContextServiceConfig.NUM_ATTRIBUTES = numAttr;
		
		switch(schemeType)
		{
			case CONTEXTNET:
			{
				ContextServiceConfig.SCHEME_TYPE = ContextServiceConfig.SchemeTypes.CONTEXTNET;
				break;
			}
			case REPLICATE_ALL:
			{
				ContextServiceConfig.SCHEME_TYPE = ContextServiceConfig.SchemeTypes.REPLICATE_ALL;
				break;
			}
			case QUERY_ALL:
			{
				ContextServiceConfig.SCHEME_TYPE = ContextServiceConfig.SchemeTypes.QUERY_ALL;
				break;
			}
			case MERCURY:
			{
				ContextServiceConfig.SCHEME_TYPE = ContextServiceConfig.SchemeTypes.MERCURY;
				break;
			}
			case HYPERDEX:
			{
				ContextServiceConfig.SCHEME_TYPE = ContextServiceConfig.SchemeTypes.HYPERDEX;
				break;
			}
			case MERCURYNEW:
			{
				ContextServiceConfig.SCHEME_TYPE = ContextServiceConfig.SchemeTypes.MERCURYNEW;
				break;
			}
			case MERCURYCONSISTENT:
			{
				ContextServiceConfig.SCHEME_TYPE = ContextServiceConfig.SchemeTypes.MERCURYCONSISTENT;
				break;
			}
		}
		
		//server_socket = new DatagramSocket(12345, InetAddress.getByName("ananas.cs.umass.edu"));
		//server_socket = new DatagramSocket(12345, InetAddress.getByName("localhost"));
		/*ReconfigurableSampleNodeConfig nc = new ReconfigurableSampleNodeConfig();
		nc.localSetup(TestConfig.getNodes());*/
		readNodeInfo();
		
		System.out.println("Number of nodes in the system "+csNodeConfig.getNodeIDs().size());
		
		nodes = new PlanetLabContextServiceExample[csNodeConfig.getNodeIDs().size()];
		
		/*for(int i=0; i<nodes.length; i++)
		{
			nodes[i] = new BasicContextServiceExample(i+CSTestConfig.startNodeID, csNodeConfig);
		}*/
		
		/*for(int i=0; i<csNodeConfig.getNodeIDs().size(); i++)
		{
			InetAddress currAddress = csNodeConfig.getNodeAddress(i+CSTestConfig.startNodeID);
			if(Utils.isMyMachineAddress(currAddress))
			{
				System.out.println("Starting context service");
				new Thread(new StartNode(i+CSTestConfig.startNodeID, i)).start();
				//nodes[i] = new BasicContextServiceExample(i+CSTestConfig.startNodeID, csNodeConfig);
			}
		}*/
		
		System.out.println("Starting context service");
		new Thread(new StartNode(myID, myID-CSTestConfig.startNodeID)).start();
		
		// print state after 10 seconds
		//FIXME: remove this sleep. After ContextServiceNode sleep is removed.
		try
		{
			Thread.sleep(120000);
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		
		/*for(int i=0; i<nodes.length; i++)
		{
			InetAddress currAddress = csNodeConfig.getNodeAddress(i+CSTestConfig.startNodeID);
			if(Utils.isMyMachineAddress(currAddress))
			{
				System.out.println("printing state for node "+i);
				nodes[i].printNodeState();
			}
		}*/
		
		System.out.println("printing state for node "+myID);
		nodes[myID-CSTestConfig.startNodeID].printNodeState();
		
		/*try
		{
			Thread.sleep(2000);
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		// store dummy value
		nodes[0].fillDummyGUIDValues();
		try
		{
			Thread.sleep(5000);
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		nodes[0].enterQueries();*/
		/*if(nodes[0]!=null)
		{
			nodes[0].enterAndMonitorQuery();
		}*/
	}
	
	private static void readNodeInfo() throws NumberFormatException, UnknownHostException, IOException
	{
		csNodeConfig = new CSNodeConfig<Integer>();
		
		BufferedReader reader = new BufferedReader(new FileReader(configFileName));
		String line = null;
		while ((line = reader.readLine()) != null)
		{
			String [] parsed = line.split(" ");
			int readNodeId = Integer.parseInt(parsed[0])+CSTestConfig.startNodeID;
			InetAddress readIPAddress = InetAddress.getByName(parsed[1]);
			int readPort = Integer.parseInt(parsed[2]);
			
			csNodeConfig.add(readNodeId, new InetSocketAddress(readIPAddress, readPort));
		}
		reader.close();
	}
	
	private static class StartNode implements Runnable
	{
		private final int nodeID;
		private final int myIndex;
		public StartNode(Integer givenNodeID, int index)
		{
			this.nodeID = givenNodeID;
			this.myIndex = index;
		}
		
		@Override
		public void run()
		{
			try
			{
				nodes[myIndex] = new PlanetLabContextServiceExample(nodeID, csNodeConfig);
				new Thread(new NumMessagesPerSec(nodes[myIndex])).start();
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	
	private static class NumMessagesPerSec implements Runnable
	{
		private final PlanetLabContextServiceExample csObj;
		
		public NumMessagesPerSec(PlanetLabContextServiceExample csObj)
		{
			this.csObj = csObj;
		}
		
		@Override
		public void run() 
		{
			long lastNumMesgs = 0;
			while(true)
			{
				try 
				{
					Thread.sleep(10000);
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
				
				long curr = csObj.contextservice.getNumMesgInSystem();
				long number = (curr-lastNumMesgs);
				lastNumMesgs = curr;
				if(number!=0)
				{
					System.out.println("ID "+csObj.myID+" NUM MESSAGES PER SEC "+number/10+" ");
				}
				this.csObj.contextservice.getContextServiceDB().getDatabaseSize();
			}
		}
	}
}


//private void fillDummyGUIDValues() throws IOException
//{	
//	System.out.println("fillDummyGUIDValues");
//	
//	BufferedReader reader = new BufferedReader(new FileReader("dummyValues.txt"));
//	String line = null;
//	while ( (line = reader.readLine()) != null )
//	{
//		String [] parsed = line.split(" ");
//		String GUID = parsed[0];
//		for(int i=1;i<parsed.length; i++)
//		{
//			String attName = ContextServiceConfig.CONTEXT_ATTR_PREFIX+"ATT"+(i-1);
//			double attrValue = Double.parseDouble(parsed[i]);
//			ValueUpdateMsgToMetadataNode<Integer> valueUpdMsgToMetanode = 
//					new ValueUpdateMsgToMetadataNode<Integer>(this.getContextService().getMyID(), 0, GUID, attName, attrValue, attrValue, null);
//			
//			Integer respMetadataNodeId = this.getContextService().getResponsibleNodeId(attName);
//			//nioTransport.sendToID(respMetadataNodeId, valueMeta.getJSONMessage());
//			
//			GenericMessagingTask<Integer, ValueUpdateMsgToMetadataNode<Integer>> mtask = 
//					new GenericMessagingTask<Integer, ValueUpdateMsgToMetadataNode<Integer>>(respMetadataNodeId, 
//							valueUpdMsgToMetanode);
//			
//			// send the message 
//			try
//			{
//				this.getContextService().getJSONMessenger().send(mtask);
//			} catch (JSONException e)
//			{
//				e.printStackTrace();
//			}
//		}
//	}
//}


/*private void enterQueries()
{	
	while (true)
	{
		  //  prompt the user to enter their name
	      System.out.print("\n\n\nEnter Queries here: ");
	 
	      //  open up standard input
	      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	 
	      String query = null;
	 
	      //  read the username from the command-line; need to use try/catch with the
	      //  readLine() method
	      
	      try 
	      {
			query = br.readLine();
	      } catch (IOException e1) 
	      {
			e1.printStackTrace();
	      }
	      
	      QueryMsgFromUser<Integer> queryMsgFromUser = 
	        	new QueryMsgFromUser<Integer>(this.getContextService().getMyID(), query);
	      // nioTransport.sendToID(0, queryMesg.getJSONMessage());
	         
	      GenericMessagingTask<Integer, QueryMsgFromUser<Integer>> mtask = 
	    		  new GenericMessagingTask<Integer, QueryMsgFromUser<Integer>>
	      (this.getContextService().getMyID(), queryMsgFromUser);
				
	      // send the message 
	      try 
	      {
	    	  this.getContextService().getJSONMessenger().send(mtask);
	      } catch (JSONException e) 
	      {
	    	  e.printStackTrace();
	      } catch (IOException e) 
	      {
	    	  e.printStackTrace();
	      }
	      //System.out.println("Thanks for the name, " + userName);
   }
}

private void enterAndMonitorQuery()
{	
	while (true)
	{
		  //  prompt the user to enter their name
	      System.out.print("\n\n\nEnter Queries here: ");
	 
	      //  open up standard input
	      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	 
	      String query = null;
	 
	      //  read the username from the command-line; need to use try/catch with the
	      //  readLine() method
	      
	      try
	      {
			query = br.readLine();
	      } catch (IOException e1) 
	      {
			e1.printStackTrace();
	      }
	      QueryMsgFromUser<Integer> queryMsgFromUser = 
	        	new QueryMsgFromUser<Integer>(this.getContextService().getMyID(), query);
	      // nioTransport.sendToID(0, queryMesg.getJSONMessage());
	         
	      GenericMessagingTask<Integer, QueryMsgFromUser<Integer>> mtask = 
	    		  new GenericMessagingTask<Integer, QueryMsgFromUser<Integer>>
	      (this.getContextService().getMyID(), queryMsgFromUser);
				
	      // send the message 
	      try 
	      {
	    	  this.getContextService().getJSONMessenger().send(mtask);
	      } catch (JSONException e) 
	      {
	    	  e.printStackTrace();
	      } catch (IOException e) 
	      {
			e.printStackTrace();
	      }
	}
	
	//System.out.println("Thanks for the name, " + userName);
	//GNSCalls.clearNotificationSetOfAGroup(query);
	//GNSCalls.updateNotificationSetOfAGroup((InetSocketAddress)server_socket.getLocalSocketAddress(), query);
	//recvNotification(query);
}*/


/*public static void recvNotification(String query)
{
   byte[] receive_data = new byte[1024];
   
   System.out.println ("UDPServer Waiting for client");
   while(true)
   {
	   DatagramPacket receive_packet = new DatagramPacket(receive_data,
                                        receive_data.length);
	   try
	   {
		   server_socket.receive(receive_packet);
	   } catch (IOException e) 
	   {
		   e.printStackTrace();
	   }
       
	   String data = new String(receive_packet.getData(),0, 0
                                     ,receive_packet.getLength());
            
       InetAddress IPAddress = receive_packet.getAddress();
             
       System.out.println("\n\n"+data+"\n\n" );
       
       JSONArray res = GNSCalls.readGroupMembers(query);
	   if(res!=null)
	   {
	   		System.out.println("\n\n query res "+ res);
	   }
	   else
	   {
	   		System.out.println("\n\n query res null" );
	   }
	}
}*/