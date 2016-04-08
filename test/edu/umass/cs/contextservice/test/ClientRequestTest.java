package edu.umass.cs.contextservice.test;


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.client.ContextServiceClient;


public class ClientRequestTest
{
	public static final String CLIENT_GUID_PREFIX							= "clientGUID";
	
	public static final String latitudeAttrName								= "geoLocationCurrentLat";
	public static final String longitudeAttrName							= "geoLocationCurrentLong";
	
	public static final int UPDATE											= 1;
	public static final int GET												= 2;
	public static final int SEARCH											= 3;
	
	public static int NUMGUIDs												= 100;
	
	public static String writerName;
	
	public static int requestID 											= 0;
	
	private ContextServiceClient<String> contextClient						= null;
	
	public static void startRequests(String csHost, int csPort) throws Exception
	{
		writerName = "writer1";
		
		
		ClientRequestTest basicObj = new ClientRequestTest(csHost, csPort);
		sendAMessage(basicObj, UPDATE);
		
		sendAMessage(basicObj, GET);
		System.out.println("Get tests pass");
		sendAMessage(basicObj, SEARCH);
	}
	
	private static void sendAMessage(ClientRequestTest basicObj, int reqType)
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
	
	public ClientRequestTest(String csHost, int csPort) throws Exception
	{
		requestID = 0;
		contextClient = new ContextServiceClient<String>(csHost, csPort);
	}
	
	/**
	 * This function sends update
	 */
	public void sendGet(long currID, int guidNum)
	{
		String memberAlias = CLIENT_GUID_PREFIX;
		String realAlias = memberAlias+guidNum;
		String myGUID = getSHA1(realAlias);
		
		JSONObject replyJSON = contextClient.sendGetRequest(myGUID);
		System.out.println("GET GUID "+myGUID+" JSON "+replyJSON);
		if(replyJSON.length() <= 0)
		{
			System.out.println("GET test fail");
			assert(false);
		}
	}
	
	/**
	 * This function sends update
	 */
	public void sendUpdate( long currID, int guidNum )
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
			attrValuePair.put(latitudeAttrName, latitude);
			attrValuePair.put(longitudeAttrName, longitude);
			
			// true fo blocking update
			contextClient.sendUpdate(myGUID, attrValuePair, currID, true);
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
		String query = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE "
				+ "geoLocationCurrentLat >= -90 AND geoLocationCurrentLong <= 180";
		//JSONObject geoJSONObject = getGeoJSON();
		//String query = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE GeojsonOverlap("+geoJSONObject.toString()+")";
		//String query = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE latitude >= ";
		JSONArray resultArray = new JSONArray();
		contextClient.sendSearchQuery(query, resultArray, 300000);
		System.out.println("Query result size "+resultArray.length());
		if(resultArray.length() == NUMGUIDs)
		{
			System.out.println("Search test pass");
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
	
	public static void main(String[] args)
	{
		String csHost = args[0];
		int csPort = Integer.parseInt(args[1]);
		try 
		{
			ClientRequestTest.startRequests(csHost, csPort);
		} catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
	
	/*private JSONObject getGeoJSON()
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
//			 JSONArray coordArray = geoJSON.getJSONArray("coordinates");
//			 JSONArray newArray = new JSONArray(coordArray.getString(0));
//			 for(int i=0;i<newArray.length();i++)
//			 {
//				 JSONArray coordList = new JSONArray(newArray.getString(i));
//				 ContextServiceLogger.getLogger().fine("i "+i+coordList.getDouble(0) );
//			 }
		} catch (JSONException e) 
		{
			e.printStackTrace();
		}
		return geoJSON;
	}*/
}