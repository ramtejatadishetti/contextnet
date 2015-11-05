package edu.umass.cs.contextservice.updateClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.AttributeTypes;
import edu.umass.cs.contextservice.CSNodeConfig;
import edu.umass.cs.contextservice.ContextServiceDemultiplexer;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONNIOTransport;

public class BasicContextUpdateNoGNS
{
	public static String csServerName 									= "ananas.cs.umass.edu";
	public static int csPort 											= 5000;
	
	// per 1000msec
	public static int ATTR_UPDATE_RATE								= 5000;
	
	//public static final int NUMGUIDs									= 1;
	// prefix for client GUIDs clientGUID1, client GUID2, ...
	public static final String CLIENT_GUID_PREFIX					= "clientGUID";
	
	public static final Random rand 									= new Random();
	// stores the current values
	private static final HashMap<String, Double> attrValueMap		= new HashMap<String, Double>();
	private static int versionNum										= 0;
	
	private static InetAddress myIP;
	private static int myPort;
	
	private static Integer myID;
	private static CSNodeConfig<Integer> csNodeConfig;
	private static JSONNIOTransport<Integer> niot;
	
	/**
	 * creates GUID and adds all attributes
	 */
	public static String createGUID(int clientID)
	{
		String guidAlias = CLIENT_GUID_PREFIX+clientID;
		String guidString = Utils.getSHA1(guidAlias).substring(0, 20);
		
		for(int i=0;i<ContextServiceConfig.NUM_ATTRIBUTES;i++)
    	{
    		attrValueMap.put(ContextServiceConfig.CONTEXT_ATTR_PREFIX+i, (double) 100);
    	}
		return guidString;
	}
	
	/**
	 * does the attribute updates at a rate.
	 * @throws JSONException 
	 * @throws IOException 
	 * @throws UnknownHostException 
	 */
	public static void doAttributeUpdates(String guidString, String guidAlias) throws JSONException, UnknownHostException, IOException
	{
		for(int i=0;i<ContextServiceConfig.NUM_ATTRIBUTES;i++)
		{
			String attName = ContextServiceConfig.CONTEXT_ATTR_PREFIX+i;
			double nextVal = 1+rand.nextInt((int)(AttributeTypes.MAX_VALUE-AttributeTypes.MIN_VALUE));
			
			double oldValue = attrValueMap.get(attName);
			attrValueMap.put(attName, nextVal);
			JSONObject allAttr = new JSONObject();
			Iterator<String> iter = attrValueMap.keySet().iterator();
			while(iter.hasNext())
			{
				String currKey = iter.next();
				allAttr.put(currKey, attrValueMap.get(currKey));
			}
			
			ValueUpdateFromGNS<Integer> valMsg = new ValueUpdateFromGNS<Integer>(myID, versionNum++, guidString,
					allAttr, myIP.getHostAddress(), 1000);
			
			niot.sendToAddress(new InetSocketAddress(InetAddress.getByName(csServerName), csPort)
			, valMsg.toJSONObject());
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
		myID = id;
		
		csNodeConfig = new CSNodeConfig<Integer>();
		
		myIP = Utils.getActiveInterfaceInetAddresses().get(0);
		myPort = 9189;
		
		csNodeConfig.add(myID, new InetSocketAddress(myIP, myPort));
		
		AbstractJSONPacketDemultiplexer pd = new ContextServiceDemultiplexer();
		//MessageExtractor worker = new MessageExtractor(pd);
		
		niot = new JSONNIOTransport<Integer>(myID, csNodeConfig, pd, true);
        new Thread(niot).start();
        
		
		int clientID = Integer.parseInt(args[2]);
		
		ATTR_UPDATE_RATE = Integer.parseInt(args[3]);
		
		String guidAlias = CLIENT_GUID_PREFIX+clientID;
		String guidString = createGUID(clientID);
		
		while(true)
		{
			try 
			{
				doAttributeUpdates(guidString, guidAlias);
				Thread.sleep(ATTR_UPDATE_RATE);
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
	}
	
}