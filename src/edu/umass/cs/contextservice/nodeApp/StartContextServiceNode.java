package edu.umass.cs.contextservice.nodeApp;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import edu.umass.cs.contextservice.ContextServiceNode;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.nio.interfaces.NodeConfig;

/**
 * Planetlab context service example with 10 nodes, with simple
 * conjunction query.
 * @author adipc
 */
public class StartContextServiceNode extends ContextServiceNode<Integer>
{
	public static final int HYPERSPACE_HASHING							= 1;
	
	
	private static CSNodeConfig<Integer> csNodeConfig					= null;
	
	//private static DatagramSocket server_socket;
	
	private static StartContextServiceNode[] nodes						= null;
	
	public StartContextServiceNode(Integer id, NodeConfig<Integer> nc)
			throws IOException
	{
		super(id, nc);
	}
	
	public static void main(String[] args) throws NumberFormatException, UnknownHostException, IOException
	{
		Integer myID = Integer.parseInt(args[0]);
		//int schemeType = Integer.parseInt(args[1]);
		//int numAttr = Integer.parseInt(args[2]);
		//ContextServiceConfig.NUM_ATTRIBUTES = numAttr;
		
		
		ContextServiceConfig.SCHEME_TYPE = ContextServiceConfig.SchemeTypes.HYPERSPACE_HASHING;
		
		
		//server_socket = new DatagramSocket(12345, InetAddress.getByName("ananas.cs.umass.edu"));
		//server_socket = new DatagramSocket(12345, InetAddress.getByName("localhost"));
		/*ReconfigurableSampleNodeConfig nc = new ReconfigurableSampleNodeConfig();
		nc.localSetup(TestConfig.getNodes());*/
		readNodeInfo();
		
		ContextServiceLogger.getLogger().fine("Number of nodes in the system "+csNodeConfig.getNodeIDs().size());
		
		nodes = new StartContextServiceNode[csNodeConfig.getNodeIDs().size()];
		
		
		ContextServiceLogger.getLogger().fine("Starting context service");
		new Thread(new StartNode(myID, myID)).start();
	}
	
	private static void readNodeInfo() throws NumberFormatException, UnknownHostException, IOException
	{
		csNodeConfig = new CSNodeConfig<Integer>();
		
		BufferedReader reader = new BufferedReader(new FileReader(ContextServiceConfig.nodeSetupFileName));
		String line = null;
		while ((line = reader.readLine()) != null)
		{
			String [] parsed = line.split(" ");
			int readNodeId = Integer.parseInt(parsed[0]);
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
				nodes[myIndex] = new StartContextServiceNode(nodeID, csNodeConfig);
				new Thread(new NumMessagesPerSec(nodes[myIndex])).start();
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	
	private static class NumMessagesPerSec implements Runnable
	{
		private final StartContextServiceNode csObj;
		
		public NumMessagesPerSec(StartContextServiceNode csObj)
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
					ContextServiceLogger.getLogger().fine("ID "+csObj.myID+" NUM MESSAGES PER SEC "+number/10+" ");
				}
				//this.csObj.contextservice.getContextServiceDB().getDatabaseSize();
			}
		}
	}
}


//private void fillDummyGUIDValues() throws IOException
//{	
//	ContextServiceLogger.getLogger().fine("fillDummyGUIDValues");
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
	      //ContextServiceLogger.getLogger().fine("Thanks for the name, " + userName);
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
	
	//ContextServiceLogger.getLogger().fine("Thanks for the name, " + userName);
	//GNSCalls.clearNotificationSetOfAGroup(query);
	//GNSCalls.updateNotificationSetOfAGroup((InetSocketAddress)server_socket.getLocalSocketAddress(), query);
	//recvNotification(query);
}*/


/*public static void recvNotification(String query)
{
   byte[] receive_data = new byte[1024];
   
   ContextServiceLogger.getLogger().fine ("UDPServer Waiting for client");
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
             
       ContextServiceLogger.getLogger().fine("\n\n"+data+"\n\n" );
       
       JSONArray res = GNSCalls.readGroupMembers(query);
	   if(res!=null)
	   {
	   		ContextServiceLogger.getLogger().fine("\n\n query res "+ res);
	   }
	   else
	   {
	   		ContextServiceLogger.getLogger().fine("\n\n query res null" );
	   }
	}
}*/