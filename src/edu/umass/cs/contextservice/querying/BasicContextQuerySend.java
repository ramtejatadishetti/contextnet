package edu.umass.cs.contextservice.querying;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.CSNodeConfig;
import edu.umass.cs.contextservice.ContextServiceDemultiplexer;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.InterfacePacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;

/**
 * Class is used to send queries to context service.
 * The node where this class runs doesn't need
 * to be part of the contextservice mesh (nodes specified in nodeConfig).
 * It is basic because it takes input from the user, doesn't use any workload.
 * @author adipc
 *
 */
public class BasicContextQuerySend<NodeIDType> implements InterfacePacketDemultiplexer<JSONObject>
{
	public static String csServerName 					= "ananas.cs.umass.edu";
	public static int csPort 							= 5000;
	
	public static final int LISTEN_PORT				= 9189;
	
	public static int requestCounter					= 0;
	
	// trigger mesg type
	//public static final int QUERY_MSG_FROM_USER 		= 2;
	//private enum Keys {QUERY};
	
	private final NodeIDType myID;
	private final CSNodeConfig<NodeIDType> csNodeConfig;
	private final JSONNIOTransport<NodeIDType> niot;
	private final String sourceIP;
	
	private static final Random rand = new Random();
	
	public BasicContextQuerySend(NodeIDType id) throws IOException
	{
		myID = id;
		
		csNodeConfig = new CSNodeConfig<NodeIDType>();
		
		sourceIP =  Utils.getActiveInterfaceInetAddresses().get(0).getHostAddress();
		
		System.out.println("Source IP address "+sourceIP);
		
		csNodeConfig.add(myID, new InetSocketAddress(sourceIP, LISTEN_PORT));
        
        AbstractJSONPacketDemultiplexer pd = new ContextServiceDemultiplexer();
		//ContextServicePacketDemultiplexer pd;
		
		System.out.println("\n\n node IP "+csNodeConfig.getNodeAddress(this.myID)+
				" node Port "+csNodeConfig.getNodePort(this.myID)+" nodeID "+this.myID);
		
		niot = new JSONNIOTransport<NodeIDType>(this.myID,  csNodeConfig, pd , true);
		
		JSONMessenger<NodeIDType> messenger = 
			new JSONMessenger<NodeIDType>(niot);
		
		pd.register(ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY, this);
		messenger.addPacketDemultiplexer(pd);	
	}
	
	public void stopThis()
	{
		this.niot.stop();
	}
	
	public void sendQueryToContextService(String query, int numAttr)
	{
		try
		{
			int userReqNum = requestCounter++;
			System.out.println("CONTEXTSERVICE EXPERIMENT: QUERYFROMUSER REQUEST ID "
					+ userReqNum +" NUMATTR "+numAttr+" AT "+System.currentTimeMillis()
					+" "+"contextATT0"+" QueryStart "+System.currentTimeMillis());
			
			QueryMsgFromUser<NodeIDType> qmesgU 
				= new QueryMsgFromUser<NodeIDType>(myID, query, sourceIP, LISTEN_PORT, userReqNum);
			//JSONObject csQuery = new JSONObject();
			//csQuery.put(JSONPacket.PACKET_TYPE, QUERY_MSG_FROM_USER);
			//csQuery.put(Keys.QUERY.toString(), query);
			
			//System.out.println("\n\nSending query to contextservice "+csQuery+"\n\n");
			//InterfaceJSONNIOTransport<NodeIDType> nioObject = new JSONNIOTransport<NodeIDType>(myID, csNodeConfig);
			
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
	
	public void handleQueryReply(JSONObject jso)
	{
		try 
		{
			long time = System.currentTimeMillis();
			QueryMsgFromUserReply<NodeIDType> qmur;
			qmur = new QueryMsgFromUserReply<NodeIDType>(jso);
			System.out.println("CONTEXTSERVICE EXPERIMENT: QUERYFROMUSERREPLY REQUEST ID "
					+qmur.getUserReqNum()+" NUMATTR "+0+" AT "+time+" EndTime "
					+time+ " QUERY ANSWER "+qmur.getResultGUIDs());
		} catch (JSONException e) 
		{
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws IOException
	{
		if(args.length >= 2)
		{
			csServerName = args[0];
			csPort = Integer.parseInt(args[1]);
		}
		Integer id = 0;
		BasicContextQuerySend<Integer> basicObj = new BasicContextQuerySend<Integer>(id);
		for(int i=0;i<10;i++)
		{
			//simpleQueryWorkload(basicObj);
			//largeQueryWorkload(basicObj);
			//queryEachAttributeWorkload(basicObj);
			randomizedQueryWorkload(basicObj);
		}
		basicObj.stopThis();
	}
	
	/**
	 * takes queries input from user
	 */
	public static void userEnteringQueries(BasicContextQuerySend<Integer> basicObj)
	{
		while(true)
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
		      basicObj.sendQueryToContextService(query, 1);
		}
	}
	
	/**
	 * sends 10 queries 10 seconds apart, with increasing number of attributes
	 * @param basicObj
	 */
	public static void simpleQueryWorkload(BasicContextQuerySend<Integer> basicObj)
	{
	      String query="";
	      for(int i=0;i<10;i++)
	      {
	    	  String predicate = "1 <= contextATT"+i+" <= 5";
	    	  if(i==0)
	    	  {
	    		  query = predicate;
	    	  }
	    	  else 
	    	  {
	    		  query = query + " && "+predicate;
	    	  }
	    	  basicObj.sendQueryToContextService(query, (i+1));
	    	  
	    	  try
	    	  {
				Thread.sleep(5000);
	    	  } catch (InterruptedException e)
	    	  {
				e.printStackTrace();
	    	  }
	      }
	}
	
	/**
	 * sends 10 queries 10 seconds apart, with increasing number of attributes
	 * @param basicObj
	 */
	public static void largeQueryWorkload(BasicContextQuerySend<Integer> basicObj)
	{
		int startNumAttr = 100;
		for(int i=0;i<10;i++)
		{
			String query = getQueryOfSize(startNumAttr);
			
			basicObj.sendQueryToContextService(query, startNumAttr);
			
	    	try
	    	{
	    		Thread.sleep(5000);
	    	} catch (InterruptedException e)
	    	{
	    		e.printStackTrace();
	    	}
	    	startNumAttr-=10;
		}
	}
	
	/**
	 * sends 10 queries 10 seconds apart, with increasing number of attributes
	 * @param basicObj
	 */
	public static void randomizedQueryWorkload(BasicContextQuerySend<Integer> basicObj)
	{
		int startNumAttr = 1;
		for(int i=0;i<15;i++)
		{
			String query = getQueryOfSize(startNumAttr);
			
			basicObj.sendQueryToContextService(query, startNumAttr);
			
	    	try
	    	{
	    		Thread.sleep(3000);
	    	} catch (InterruptedException e)
	    	{
	    		e.printStackTrace();
	    	}
	    	startNumAttr = startNumAttr+2;
		}
	}
	
	public static String getQueryOfSize(int queryLength)
	{
		String query="";
	    for(int i=0;i<queryLength;i++)
	    {
	    	int attrNum = rand.nextInt(ContextServiceConfig.NUM_ATTRIBUTES);
	    	
	    	String predicate = "1 <= contextATT"+attrNum+" <= 5";
	    	if(i==0)
	    	{
	    		query = predicate;
	    	}
	    	else
	    	{
	    		query = query + " && "+predicate;
	    	}
	    }
	    return query;
	}
	
	public static void queryEachAttributeWorkload(BasicContextQuerySend<Integer> basicObj)
	{
		//String query="";
		for(int i=0;i<ContextServiceConfig.NUM_ATTRIBUTES;i++)
		{
			String predicate = "1 <= contextATT"+i+" <= 5";
			
			basicObj.sendQueryToContextService(predicate, 1);
			
			try
			{
				Thread.sleep(5000);
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}

	@Override
	public boolean handleMessage(JSONObject jsonObject) 
	{
		System.out.println("QuerySourceDemux JSON packet recvd "+jsonObject);
		try
		{
			if(jsonObject.getInt(ContextServicePacket.PACKET_TYPE) 
					== ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY.getInt())
			{
				System.out.println("JSON packet recvd "+jsonObject);
				handleQueryReply(jsonObject);
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		return true;
	}
}