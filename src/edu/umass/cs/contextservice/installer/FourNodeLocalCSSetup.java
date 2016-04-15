package edu.umass.cs.contextservice.installer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.ContextServiceNode;
import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.config.CSConfigFileLoader;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.geodesy.GlobalCoordinate;
import edu.umass.cs.contextservice.queryparsing.GeoJSON;
import edu.umass.cs.nio.interfaces.NodeConfig;


/**
 * Just a simple four node CS setup for local testing.
 * @author adipc
 *
 */
public class FourNodeLocalCSSetup extends ContextServiceNode<Integer>
{
	public static final int HYPERSPACE_HASHING							= 1;
	
	private static CSNodeConfig<Integer> csNodeConfig					= null;
	
	private static FourNodeLocalCSSetup[] nodes							= null;
	
	private static ContextServiceClient<Integer> csClient;
	
	private static int numStarted = 0;
	
	private static final Object startedLock = new Object();
	
	public FourNodeLocalCSSetup(Integer id, NodeConfig<Integer> nc)
			throws Exception
	{
		super(id, nc);
		waitToFinishStart();
		
		synchronized(startedLock)
		{
			numStarted++;
			
			if( numStarted == nc.getNodeIDs().size() )
			{
				startedLock.notify();;
			}
		}
	}
	
	public static void main(String[] args) throws NumberFormatException, UnknownHostException, IOException
	{
		ContextServiceConfig.configFileDirectory = "conf/singleNodeConf/contextServiceConf";
//		ContextServiceConfig.configFileDirectory 
//		= "/home/adipc/Documents/MobilityFirstGitHub/ContextNet/contextnet/conf/singleNodeConf/contextServiceConf";
		ContextServiceConfig.SCHEME_TYPE = ContextServiceConfig.SchemeTypes.HYPERSPACE_HASHING;		
		
		CSConfigFileLoader configFileLoader = new CSConfigFileLoader(
				ContextServiceConfig.configFileDirectory+"/"+ContextServiceConfig.csConfigFileName);
		
		readNodeInfo();
		
		System.out.println("Number of nodes in the system "+csNodeConfig.getNodeIDs().size());
		
		nodes = new FourNodeLocalCSSetup[csNodeConfig.getNodeIDs().size()];
		
		System.out.println("Starting context service 0");
		new Thread(new StartNode(0, 0)).start();
		
		System.out.println("Starting context service 1");
		new Thread(new StartNode(1, 1)).start();
		
		System.out.println("Starting context service 2");
		new Thread(new StartNode(2, 2)).start();
		
		System.out.println("Starting context service 3");
		new Thread(new StartNode(3, 3)).start();
		
		while(numStarted != csNodeConfig.getNodeIDs().size())
		{
			synchronized(startedLock)
			{
				try {
					startedLock.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		System.out.println("\n\n########### All context service nodes have started#############\n\n");
		
//		csClient = new ContextServiceClient<Integer>( csNodeConfig.getNodeAddress(0).getHostAddress(), 
//				csNodeConfig.getNodePort(0) );
//		
//		try {
//			RequestClass.startRequests();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
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
				nodes[myIndex] = new FourNodeLocalCSSetup(nodeID, csNodeConfig);
				
			} catch ( Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
	private static class RequestClass
	{
		public static final String CLIENT_GUID_PREFIX							= "clientGUID";
		
		public static final int UPDATE											= 1;
		public static final int GET												= 2;
		public static final int SEARCH											= 3;
		
		public static int NUMGUIDs												= 100;
		
		public static int requestID 											= 0;
		
		public static void startRequests() throws Exception
		{	
			RequestClass basicObj = new RequestClass();
			sendAMessage(basicObj, SEARCH);
			
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
		
		/**
		 * This function sends update
		 */
		public void sendGet(long currID, int guidNum)
		{
			String memberAlias = CLIENT_GUID_PREFIX;
			String realAlias = memberAlias+guidNum;
			String myGUID = getSHA1(realAlias);
			
			long startTime = System.currentTimeMillis();
			JSONObject replRecvd = csClient.sendGetRequest(myGUID);
			System.out.println("Get completion requestID "+requestID+
					" time "+(System.currentTimeMillis()-startTime)+" size "+replRecvd.length());
		}
		
		/**
		 * This function sends update
		 */
		public void sendUpdate(long currID, int guidNum)
		{	
			String memberAlias = CLIENT_GUID_PREFIX;
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
				
				long startTime = System.currentTimeMillis();
				csClient.sendUpdate(myGUID, null, attrValuePair, currID, true);
				System.out.println("Update completion requestID "+requestID+" time "
						+(System.currentTimeMillis()-startTime));
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		/**
		 * Depending on the random outcome this function sends query
		 */
		public void sendQuery(long currID)
		{
			String query = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE geoLocationCurrentLat >= -80 AND geoLocationCurrentLong <= 170";
			//JSONObject geoJSONObject = getGeoJSON();
			//String query = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE GeojsonOverlap(geoLocationCurrentLat, geoLocationCurrentLong, "+geoJSONObject.toString()+")";
			
			//eservice.execute(new SendingRequest(currID, SendingRequest.QUERY, query, currNumAttr, "", -1, -1, "") );
			//currNumAttr = currNumAttr + 2;
			JSONArray replyArray = new JSONArray();
			long startTime = System.currentTimeMillis();
			int replySize = csClient.sendSearchQuery(query, replyArray, 300000);
			System.out.println("Search query completion requestID "+requestID+" time "+(System.currentTimeMillis()-startTime)+
					" replySize "+replySize);
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