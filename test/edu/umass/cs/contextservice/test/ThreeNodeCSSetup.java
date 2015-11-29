package edu.umass.cs.contextservice.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import edu.umass.cs.contextservice.CSNodeConfig;
import edu.umass.cs.contextservice.ContextServiceNode;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.examples.basic.CSTestConfig;
import edu.umass.cs.nio.InterfaceNodeConfig;

public class ThreeNodeCSSetup extends ContextServiceNode<Integer>
{
	public static final int CONTEXTNET								= 1;
	public static final int REPLICATE_ALL							= 2;
	public static final int QUERY_ALL								= 3;
	public static final int MERCURY									= 4;
	public static final int HYPERDEX								= 5;
	public static final int MERCURYNEW								= 6;
	public static final int MERCURYCONSISTENT						= 7;
	
	
	private static CSNodeConfig<Integer> csNodeConfig					= null;
	
	//private static DatagramSocket server_socket;
	
	private static ThreeNodeCSSetup[] nodes								= null;
	
	//public static final String configFileName							= "contextServiceNodeSetup.txt";
	
	public ThreeNodeCSSetup(Integer id, InterfaceNodeConfig<Integer> nc)
			throws IOException
	{
		super(id, nc);
	}
	
	public static void main(String[] args) throws NumberFormatException, UnknownHostException, IOException
	{
		int schemeType = Integer.parseInt(args[0]);
		int numAttr = Integer.parseInt(args[1]);
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
		
		readNodeInfo();
		
		System.out.println("Number of nodes in the system "+csNodeConfig.getNodeIDs().size());
		
		nodes = new ThreeNodeCSSetup[csNodeConfig.getNodeIDs().size()];
		
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
		
		System.out.println("Starting context service 0");
		new Thread(new StartNode(0, 0-CSTestConfig.startNodeID)).start();
		
		System.out.println("Starting context service 1");
		new Thread(new StartNode(1, 1-CSTestConfig.startNodeID)).start();
		
		System.out.println("Starting context service 2");
		new Thread(new StartNode(2, 2-CSTestConfig.startNodeID)).start();
		
		try
		{
			Thread.sleep(20000);
		} catch (InterruptedException e) 
		{
			e.printStackTrace();
		}
		
		System.out.println("printing state for node "+0);
		nodes[0-CSTestConfig.startNodeID].printNodeState();
		
		System.out.println("printing state for node "+1);
		nodes[1-CSTestConfig.startNodeID].printNodeState();
		
		System.out.println("printing state for node "+2);
		nodes[2-CSTestConfig.startNodeID].printNodeState();
		
		/*for(int i=0; i<nodes.length; i++)
		{
			InetAddress currAddress = csNodeConfig.getNodeAddress(i+CSTestConfig.startNodeID);
			if(Utils.isMyMachineAddress(currAddress))
			{
				System.out.println("printing state for node "+i);
				nodes[i].printNodeState();
			}
		}*/
		
		//System.out.println("printing state for node "+myID);
		//nodes[myID-CSTestConfig.startNodeID].printNodeState();
		
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
		csNodeConfig.add(0, new InetSocketAddress("127.0.0.1", 8001));
		csNodeConfig.add(1, new InetSocketAddress("127.0.0.1", 8002));
		csNodeConfig.add(2, new InetSocketAddress("127.0.0.1", 8003));
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
				nodes[myIndex] = new ThreeNodeCSSetup(nodeID, csNodeConfig);
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
	
}