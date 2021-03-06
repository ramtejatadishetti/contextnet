package edu.umass.cs.contextservice.test;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.ContextServiceNode;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.config.ContextServiceConfig.PrivacySchemes;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.GetMessage;
import edu.umass.cs.contextservice.messages.GetReplyMessage;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNSReply;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.interfaces.PacketDemultiplexer;
import edu.umass.cs.nio.nioutils.NIOHeader;

public class FourNodeCSSetupStringAttrs extends ContextServiceNode
{
	// TODO: string attr test needs to be added to context service tests
	public static final int HYPERSPACE_HASHING							= 1;
	
	private static CSNodeConfig csNodeConfig							= null;
	
	private static FourNodeCSSetupStringAttrs[] nodes					= null;
	
	
	public FourNodeCSSetupStringAttrs(Integer id, CSNodeConfig nc)
			throws Exception
	{
		super(id, nc);
	}
	
	public static void main(String[] args) throws NumberFormatException, UnknownHostException, IOException
	{	
		readNodeInfo();
		
		System.out.println("Number of nodes in the system "+csNodeConfig.getNodeIDs().size());
		
		nodes = new FourNodeCSSetupStringAttrs[csNodeConfig.getNodeIDs().size()];
		
		System.out.println("Starting context service 0");
		new Thread(new StartNode(0, 0)).start();
		
		System.out.println("Starting context service 1");
		new Thread(new StartNode(1, 1)).start();
		
		System.out.println("Starting context service 2");
		new Thread(new StartNode(2, 2)).start();
		
		System.out.println("Starting context service 3");
		new Thread(new StartNode(3, 3)).start();
		
		try
		{
			Thread.sleep(5000);
		} catch (InterruptedException e) 
		{
			e.printStackTrace();
		}
		
		try 
		{
			RequestClass.startRequests();
		} catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
	
	private static void readNodeInfo() throws NumberFormatException, UnknownHostException, IOException
	{
		csNodeConfig = new CSNodeConfig();
		
		BufferedReader reader 
				= new BufferedReader(new FileReader(ContextServiceConfig.NODE_SETUP_FILENAME));
		String line 
				= null;
		while ( (line = reader.readLine()) != null )
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
				nodes[myIndex] = new FourNodeCSSetupStringAttrs(nodeID, csNodeConfig);
			} 
			catch (Exception e) 
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
		
		public static int NUMGUIDs												= 100;
		
		private final JSONNIOTransport<Integer> niot;
		private final JSONMessenger<Integer> messenger;
		private final String sourceIP;
		private final int sourcePort;
		
		private CSNodeConfig localNodeConfig							= null;
		
		public static int requestID 											= 0;
		
		private final int myID;
		
		public static void startRequests() throws Exception
		{
			System.out.println("Starting requests");
			
			RequestClass basicObj = new RequestClass();
			sendAMessage(basicObj, UPDATE);
			
			sendAMessage(basicObj, GET);
			
			sendAMessage(basicObj, SEARCH);
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
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
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
			localNodeConfig =  new CSNodeConfig();
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
				GetMessage getMessageObj = 
						new GetMessage(myID, currID, myGUID,  sourceIP, sourcePort );
				
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
			//double latitude = (valRand.nextDouble()-0.5)*180;
			//double longitude = (valRand.nextDouble()-0.5)*360;
			String latitude  = "";
			String longitude = "";
			if(currID%2 == 0)
			{
				latitude  = "a";
				longitude = "a";
			}
			else
			{
				latitude  = "d";
				longitude = "d";
			}
			
			try
			{
				JSONObject attrValuePair = new JSONObject();
				attrValuePair.put("latitude", latitude+"");
				attrValuePair.put("longitude", longitude+"");
				
				ValueUpdateFromGNS valUpdFromGNS = 
					new ValueUpdateFromGNS
				(myID, currID, myGUID, attrValuePair,  currID, sourceIP, sourcePort, 
						System.currentTimeMillis(), null, 
						PrivacySchemes.NO_PRIVACY.ordinal(), 
						null);
				
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
			//String query = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE latitude >= '-90' AND longitude <= '180'";
			//String query   = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE latitude >= 'a' AND longitude >= 'b'";
			//String query   = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE latitude = 'd' AND longitude = 'd'";
			String query   = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE latitude >= 'a' AND longitude >= 'a'";
			//JSONObject geoJSONObject = getGeoJSON();
			//String query = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE GeojsonOverlap("+geoJSONObject.toString()+")";
			
			//eservice.execute(new SendingRequest(currID, SendingRequest.QUERY, query, currNumAttr, "", -1, -1, "") );
			//currNumAttr  = currNumAttr + 2;
			
			QueryMsgFromUser qmesgU 
			= new QueryMsgFromUser(myID, query, currID, 
					300000, sourceIP, sourcePort, PrivacySchemes.NO_PRIVACY.ordinal());
			
			InetSocketAddress sockAddr = getRandomNodeSock();
			//ContextServiceLogger.getLogger().fine("Sending query to "+sockAddr);
			
			try 
			{
				niot.sendToAddress(sockAddr, qmesgU.toJSONObject());
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		public void handleUpdateReply(JSONObject jso)
		{
			ValueUpdateFromGNSReply vur;
			try
			{
				vur = new ValueUpdateFromGNSReply(jso);
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
				GetReplyMessage getReply= new GetReplyMessage(jso);
				
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
				QueryMsgFromUserReply qmur;
				qmur = new QueryMsgFromUserReply(jso);
				
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
		public boolean handleMessage(JSONObject jsonObject, NIOHeader nioHeader)
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
					//handleRefreshTrigger(jsonObject);
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
	}
}