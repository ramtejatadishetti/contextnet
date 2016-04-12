package edu.umass.cs.contextservice.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.ContextServiceNode;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.geodesy.GlobalCoordinate;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.GetMessage;
import edu.umass.cs.contextservice.messages.GetReplyMessage;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;
import edu.umass.cs.contextservice.messages.RefreshTrigger;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNSReply;
import edu.umass.cs.contextservice.queryparsing.GeoJSON;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.interfaces.PacketDemultiplexer;

public class LargeNumNodesCSSetup extends ContextServiceNode<Integer>
{
	public static final int HYPERSPACE_HASHING							= 1;
	
	private static CSNodeConfig<Integer> csNodeConfig					= null;
	
	private static ExecutorService eservice;
	//private static FourNodeCSSetup[] nodes								= null;
	
	public LargeNumNodesCSSetup(Integer id, NodeConfig<Integer> nc)
			throws IOException
	{
		super(id, nc);
	}
	
	public static void main(String[] args) throws NumberFormatException, UnknownHostException, IOException
	{
		int numNodes = Integer.parseInt(args[0]);
		
		eservice = Executors.newCachedThreadPool();
		
		ContextServiceConfig.configFileDirectory 
			= "/home/adipc/Documents/MobilityFirstGitHub/ContextNet/contextnet/conf/singleNodeConf/contextServiceConf";
		
		ContextServiceConfig.SCHEME_TYPE = ContextServiceConfig.SchemeTypes.HYPERSPACE_HASHING;		
		
		writeConfigFiles(numNodes);
		// create contextServiceNodeConfig.txt and dbNodeConfig.txt
		
		readNodeInfo();
		
		System.out.println("Number of nodes in the system "+csNodeConfig.getNodeIDs().size());
		
		for(int i=0; i<numNodes; i++)
		{
			System.out.println("Starting context service "+i);
			eservice.execute(new StartNode(i));
		}
		
		try
		{
			Thread.sleep(30000);
		} catch (InterruptedException e) 
		{
			e.printStackTrace();
		}
		
		System.out.println("starting request load");
		try 
		{
			RequestClass.startRequests();
		} catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
	
	public static void writeConfigFiles(int numNodes) throws IOException
	{
		File file = new File(ContextServiceConfig.configFileDirectory+"/"+
				"contextServiceNodeSetup.txt");
		
		// if file doesnt exists, then create it
		if (!file.exists()) 
		{
			file.createNewFile();
		}
		
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		int initialPort = 8000;
		for( int i=0; i<numNodes; i++ )
		{
			String writeStr = i+" "+"127.0.0.1"+" "+(initialPort+i)+"\n";
			bw.write(writeStr);
		}
		bw.close();
		
		
		file = new File(ContextServiceConfig.configFileDirectory+"/"+
				"dbNodeSetup.txt");
		
		// if file doesnt exists, then create it
		if (!file.exists()) 
		{
			file.createNewFile();
		}
		
		fw = new FileWriter(file.getAbsoluteFile());
		bw = new BufferedWriter(fw);
		//initialPort = 6000;
		for( int i=0; i<numNodes; i++ )
		{
			String writeStr = i+" "+"3306"+" contextDB"+i+" root aditya\n";
			bw.write(writeStr);
		}
		bw.close();
	}
	
	private static void readNodeInfo() throws NumberFormatException, UnknownHostException, IOException
	{	
		csNodeConfig = new CSNodeConfig<Integer>();
		
		BufferedReader reader = new BufferedReader(new FileReader(
				ContextServiceConfig.configFileDirectory+"/"+ContextServiceConfig.nodeSetupFileName));
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
		public StartNode(Integer givenNodeID)
		{
			this.nodeID = givenNodeID;
		}
		
		@Override
		public void run()
		{
			try
			{
				new LargeNumNodesCSSetup(nodeID, csNodeConfig);
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	private static class RequestClass implements PacketDemultiplexer<JSONObject>
	{
		public static final String CLIENT_GUID_PREFIX							= "clientGUID";
		
		public static final int UPDATE											= 1;
		public static final int GET												= 2;
		public static final int SEARCH											= 3;
		
		public static int NUMGUIDs												= 10;
		
		private final JSONNIOTransport<Integer> niot;
		private final JSONMessenger<Integer> messenger;
		private final String sourceIP;
		private final int sourcePort;
		
		private CSNodeConfig<Integer> localNodeConfig							= null;
		
		
		private final Object replyWaitMonitor									= new Object();
			
		public static String writerName;
		
		public static int requestID 											= 0;
		
		private final int myID;
		
		public static void startRequests() throws Exception
		{
			writerName = "writer1";
			
			RequestClass basicObj = new RequestClass();
			//sendAMessage(basicObj, SEARCH);
			
			sendAMessage(basicObj, UPDATE);
			
			sendAMessage(basicObj, GET);
			
			//sendAMessage(basicObj, SEARCH);
		}
		
		private static void sendAMessage(RequestClass basicObj, int reqType)
		{			
			// send query
			if( reqType == SEARCH )
			{
				basicObj.sendQuery(requestID);
			}
			// send update
			else if( reqType == UPDATE )
			{
				for(int i=0; i< NUMGUIDs; i++)
				{
					requestID++;
					basicObj.sendUpdate(requestID, i);
					try 
					{
						Thread.sleep(100);
					} catch (InterruptedException e) 
					{
						e.printStackTrace();
					}
				}
			}
			else if( reqType == GET )
			{
				for(int i=0;i<NUMGUIDs;i++)
				{
					requestID++;
					basicObj.sendGet(requestID, i);
					try 
					{
						Thread.sleep(100);
					} catch (InterruptedException e) 
					{
						e.printStackTrace();
					}
				}
			}
		}
		
		public RequestClass() throws Exception
		{
			myID = 0;
			
			requestID = 0;
					
			sourcePort = 11111;
			
			sourceIP =  "127.0.0.1";
			
			
			System.out.println("Source IP address "+sourceIP);
			localNodeConfig =  new CSNodeConfig<Integer>();
			localNodeConfig.add(myID, new InetSocketAddress(sourceIP, sourcePort));
	        
	        AbstractJSONPacketDemultiplexer pd = new edu.umass.cs.contextservice.common.ContextServiceDemultiplexer();
			
	        System.out.println("\n\n node IP "+localNodeConfig.getNodeAddress(myID) +
					" node Port "+localNodeConfig.getNodePort(myID)+" nodeID "+myID);
			
			niot = new JSONNIOTransport<Integer>(this.myID,  localNodeConfig, pd , true);
			
			messenger = 
				new JSONMessenger<Integer>(niot);
			
			pd.register(ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY, this);
			pd.register(ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY, this);
			pd.register(ContextServicePacket.PacketType.REFRESH_TRIGGER, this);
			pd.register(ContextServicePacket.PacketType.GET_REPLY_MESSAGE, this);
			
			messenger.addPacketDemultiplexer(pd);
		}
		
		/**
		 * This function sends update
		 */
		public void sendGet(long currID, int guidNum)
		{	
			String memberAlias = CLIENT_GUID_PREFIX+myID;
			String realAlias = memberAlias+guidNum;
			String myGUID = getSHA1(realAlias);
			
			try
			{
				GetMessage<Integer> getMessageObj = 
						new GetMessage<Integer>(myID, currID, myGUID, sourceIP, sourcePort);
				
				niot.sendToAddress(getRandomNodeSock(), getMessageObj.toJSONObject());
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
		 * This function sends update
		 */
		public void sendUpdate(long currID, int guidNum)
		{	
			String memberAlias = CLIENT_GUID_PREFIX+myID;
			String realAlias = memberAlias+guidNum;
			String myGUID = getSHA1(realAlias);
			Random valRand = new Random();
			double latitude = (valRand.nextDouble()-0.5)*180;
			double longitude = (valRand.nextDouble()-0.5)*360;
			if(currID%2 == 0)
			{
				latitude = 42.466;
				longitude = -72.58;
			}
			
			try
			{
				JSONObject attrValuePair = new JSONObject();
				attrValuePair.put("geoLocationCurrentLat", latitude);
				attrValuePair.put("geoLocationCurrentLong", longitude);
				
				ValueUpdateFromGNS<Integer> valUpdFromGNS = 
					new ValueUpdateFromGNS<Integer>
				(myID, currID, myGUID, attrValuePair, currID, sourceIP, sourcePort, 
						System.currentTimeMillis());
				
				niot.sendToAddress(getRandomNodeSock(), valUpdFromGNS.toJSONObject());
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
		 * Depending on the random outcome this function sends query
		 */
		public void sendQuery(long currID)
		{
			//String query = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE latitude >= -90 AND longitude <= 180";
			JSONObject geoJSONObject = getGeoJSON();
			String query = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE GeojsonOverlap(geoLocationCurrentLat, geoLocationCurrentLong, "+geoJSONObject.toString()+")";
			
			//eservice.execute(new SendingRequest(currID, SendingRequest.QUERY, query, currNumAttr, "", -1, -1, "") );
			//currNumAttr = currNumAttr + 2;
			
			QueryMsgFromUser<Integer> qmesgU 
				= new QueryMsgFromUser<Integer>(myID, query, currID, 300000, sourceIP, sourcePort);
			
			InetSocketAddress sockAddr = getRandomNodeSock();
			//ContextServiceLogger.getLogger().fine("Sending query to "+sockAddr);
			
			try 
			{
				System.out.println("Sending search query to "+sockAddr);
				niot.sendToAddress(sockAddr, qmesgU.toJSONObject());
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		public void stopThis()
		{
			this.niot.stop();
			this.messenger.stop();
		}
		
		public void handleUpdateReply(JSONObject jso)
		{
			ValueUpdateFromGNSReply<Integer> vur;
			try
			{
				vur = new ValueUpdateFromGNSReply<Integer>(jso);
				long currReqID = vur.getVersionNum();
				
				System.out.println("Update completion requestID "+currReqID+" time "+System.currentTimeMillis());
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		private void handleGetReply(JSONObject jso)
		{
			try
			{
				GetReplyMessage<Integer> getReply= new GetReplyMessage<Integer>(jso);
				
				long reqID = getReply.getReqID();
				
				System.out.println("Get completion requestID "+reqID+" time "+System.currentTimeMillis()+" size "+getReply.getGUIDObject().length());
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		private void handleQueryReply(JSONObject jso)
		{
			try
			{
				QueryMsgFromUserReply<Integer> qmur;
				qmur = new QueryMsgFromUserReply<Integer>(jso);
				
				long reqID = qmur.getUserReqNum();
				int resultSize = qmur.getReplySize();
//				r(int i=0;i<qmur.getResultGUIDs().length();i++)
//				{
//					resultSize = resultSize+qmur.getResultGUIDs().getJSONArray(i).length();
//				}
				System.out.println("Search query completion requestID "+reqID+" time "+System.currentTimeMillis()+
						" replySize "+resultSize);
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		private void handleRefreshTrigger(JSONObject jso)
		{
			try
			{
				RefreshTrigger<Integer> qmur;
				qmur = new RefreshTrigger<Integer>(jso);
				
				long reqID = qmur.getVersionNum();
				
				System.out.println("RefreshTrigger completion requestID "
				+reqID+" time "+System.currentTimeMillis()+" "+qmur);
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		public static String getSHA1(String stringToHash)
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
		
		private InetSocketAddress getRandomNodeSock()
		{
			Random rand = new Random();
			int id = rand.nextInt(csNodeConfig.getNodeIDs().size());
			return new InetSocketAddress(csNodeConfig.getNodeAddress(id), csNodeConfig.getNodePort(id));
		}
		
		@Override
		public boolean handleMessage(JSONObject jsonObject)
		{
			try
			{
				if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE) 
						== ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY.getInt() )
				{
					handleQueryReply(jsonObject);
				} else if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE)
						== ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY.getInt() )
				{
					handleUpdateReply(jsonObject);
				} else if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE)
						== ContextServicePacket.PacketType.REFRESH_TRIGGER.getInt() )
				{
					handleRefreshTrigger(jsonObject);
				} else if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE)
						== ContextServicePacket.PacketType.GET_REPLY_MESSAGE.getInt() )
				{
					handleGetReply(jsonObject);
				}
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			return true;
		}
		
		private JSONObject getGeoJSON()
		{
			List<GlobalCoordinate> list = new LinkedList<GlobalCoordinate>();
			GlobalCoordinate amherst = new GlobalCoordinate(42.340382, -72.496819);
			GlobalCoordinate northampton = new GlobalCoordinate(42.3250896, -72.6412013);
			GlobalCoordinate sunderland = new GlobalCoordinate(42.4663727, -72.5795115);
			list.add(amherst);
			list.add(sunderland);
			list.add(northampton);
			list.add(amherst);
			JSONObject geoJSON = null;
			try 
			{
				 geoJSON = GeoJSON.createGeoJSONPolygon(list);
				 /*JSONArray coordArray = geoJSON.getJSONArray("coordinates");
				 JSONArray newArray = new JSONArray(coordArray.getString(0));
				 for(int i=0;i<newArray.length();i++)
				 {
					 JSONArray coordList = new JSONArray(newArray.getString(i));
					 ContextServiceLogger.getLogger().fine("i "+i+coordList.getDouble(0) );
				 }*/
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			return geoJSON;
		}
	}
}