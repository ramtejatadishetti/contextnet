package edu.umass.cs.contextservice.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.processing.QueryComponent;
import edu.umass.cs.contextservice.processing.QueryParser;

/**
 * Class specifies the common utility methods like SHA1 hashing etc.
 * @author ayadav
 *
 */

public class Utils
{
	private static final Logger LOGGER 				= ContextServiceLogger.getLogger();
	
	public static final String UDPServer				= "ananas.cs.umass.edu";
	public static final int UDPPort					= 54321;
	
	public static Vector<String> getActiveInterfaceStringAddresses()
	{
	    Vector<String> CurrentInterfaceIPs = new Vector<String>();
	    try
	    {
	      for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();)
	      {
	        NetworkInterface intf = en.nextElement();
	        for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr.hasMoreElements();)
	        {
	          InetAddress inetAddress = enumIpAddr.nextElement();
	          if (!inetAddress.isLoopbackAddress())
	          {
	            // FIXME: find better method to get ipv4 address
	            String IP = inetAddress.getHostAddress();
	            if (IP.contains(":")) // means IPv6
	            {
	              continue;
	            }
	            else
	            {
	              CurrentInterfaceIPs.add(IP);
	            }
	          }
	        }
	      }
	    }
	    catch (Exception ex)
	    {
	      ex.printStackTrace();
	    }
	    return CurrentInterfaceIPs;
	}

	  public static Vector<InetAddress> getActiveInterfaceInetAddresses()
	  {
	    Vector<InetAddress> CurrentInterfaceIPs = new Vector<InetAddress>();
	    try
	    {
	      for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();)
	      {
	        NetworkInterface intf = en.nextElement();
	        for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr.hasMoreElements();)
	        {
	          InetAddress inetAddress = enumIpAddr.nextElement();
	          if (!inetAddress.isLoopbackAddress())
	          {
	            // FIXME: find better method to get ipv4 address
	            String IP = inetAddress.getHostAddress();
	            if (IP.contains(":")) // means IPv6
	            {
	              continue;
	            }
	            else
	            {
	              CurrentInterfaceIPs.add(inetAddress);
	            }
	          }
	        }
	      }
	    }
	    catch (Exception ex)
	    {
	      ex.printStackTrace();
	    }
	    return CurrentInterfaceIPs;
	  }
	
	/**
	   * convert byte[] GUID into String rep of hex, for indexing at proxy
	   * 
	   * @param a
	   * @return
	   */
	  public static String bytArrayToHex(byte[] a)
	  {
	    StringBuilder sb = new StringBuilder();

	    for (byte b : a)
	      sb.append(String.format("%02x", b & 0xff));
	    
	    String toBeReturned = sb.toString();
	    toBeReturned = toBeReturned.toUpperCase();
	    return toBeReturned;
	  }
	  
	  public static byte[] hexStringToByteArray(String s) 
	  {
		  int len = s.length();
		  byte[] data = new byte[len / 2];
		  for (int i = 0; i < len; i += 2) 
		  {
			  data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
					  + Character.digit(s.charAt(i+1), 16));
		  }
		  return data;
	  }
	  
	  /**
	   * takes list of elements as input and returns the
	   * conjunction over list of elements.
	   * @param elements
	   * @return
	   */
	  public static JSONArray doConjuction(LinkedList<LinkedList<String>> elements)
	  {
		  LinkedList<String> result = new LinkedList<String>();
		  int numPredicates = elements.size();
		  //System.out.println(" numPredicates "+numPredicates);
		  HashMap<String, Integer> conjuctionMap = new HashMap<String, Integer>();
		  for (int i=0;i<elements.size();i++)
		  {
			  LinkedList<String> currList = elements.get(i);
			  for(int j=0;j<currList.size();j++)
			  {
				  String curString = currList.get(j);
				  Integer count = conjuctionMap.get(curString);
				  if(count == null)
				  {
					  //System.out.println("Key "+curString);
					  conjuctionMap.put(curString, 1);
				  } 
				  else
				  {
					  //System.out.println("Key++ "+curString+", "+count+1);
					  conjuctionMap.put(curString, count+1);
				  }
			  }
		  }
		  
		  for (Map.Entry<String, Integer> entry : conjuctionMap.entrySet()) 
		  {
			    String key = entry.getKey();
			    Integer value = entry.getValue();
			    if(value == numPredicates)
			    {
			    	result.add(key);
			    }
		  }
		  
		  JSONArray resultJSON = new JSONArray();
		  
		  for(int i=0;i<result.size();i++)
		  {
			  resultJSON.put(result.get(i));
		  }
		  //System.out.println("conjuctionMap "+conjuctionMap);
		  return resultJSON;
	  }
	  
	  
	  public static JSONArray doDisjuction(LinkedList<LinkedList<String>> elements)
	  {
		  LinkedList<String> result = new LinkedList<String>();
		  //System.out.println(" numPredicates "+numPredicates);
		  HashMap<String, Integer> disjunctionMap = new HashMap<String, Integer>();
		  for (int i=0;i<elements.size();i++)
		  {
			  LinkedList<String> currList = elements.get(i);
			  for(int j=0;j<currList.size();j++)
			  {
				  String curString = currList.get(j);
				  Integer count = disjunctionMap.get(curString);
				  if(count == null)
				  {
					  //System.out.println("Key "+curString);
					  disjunctionMap.put(curString, 1);
				  } 
				  else
				  {
					  //System.out.println("Key++ "+curString+", "+count+1);
					  disjunctionMap.put(curString, count+1);
				  }
			  }
		  }
		  
		  result.addAll(disjunctionMap.keySet());
		  
		  JSONArray resultJSON = new JSONArray();
		  
		  for(int i=0;i<result.size();i++)
		  {
			  resultJSON.put(result.get(i));
		  }
		  //System.out.println("conjuctionMap "+conjuctionMap);
		  return resultJSON;
	  }
	  
	  public static  List<String> JSONArayToList(JSONArray jsonArr)
	  {
		  List<String> returnList = new LinkedList<String>();
		  
		  for(int i=0;i<jsonArr.length();i++)
		  {
			  try 
			  {
				  returnList.add(jsonArr.getString(i));
			  } catch (JSONException e) 
			  {
				e.printStackTrace();
			  }
		  }
		  return returnList;
	  }
	  
	/**
	 * checks if the input attr value lies within the range
	 * @return
	 */
	public static boolean checkQCForOverlapWithValue(double attrValue, QueryComponent qc)
	{
		if( (qc.getLeftValue() <= attrValue) && (qc.getRightValue() > attrValue) )
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	/**
	 * Takes all the context attributes, new and the old values, 
	 * checks if the group query is still satisfied or not.
	 * @return
	 * @throws JSONException 
	 * @throws NumberFormatException 
	 */
	public static boolean groupMemberCheck(JSONObject allAttr, String updateAttrName
			, double attrVal, String groupQuery)
			throws JSONException
	{
		boolean satisfiesGroup = true;
		Vector<QueryComponent> groupQC = QueryParser.parseQuery(groupQuery);
		
		for(int j=0;j<groupQC.size();j++)
		{
			QueryComponent qc = groupQC.get(j);
			String attrName = qc.getAttributeName();
			
			// if this is the case, don't use the given val
			if(attrName.equals(updateAttrName))
			{
				// values are indexed by attr names
				double recValue = attrVal;
				boolean checkRes = Utils.checkQCForOverlapWithValue(recValue, qc);
				
				// if not satisfies, then group not satisfied
				if(!checkRes)
				{
					satisfiesGroup = false;
					break;
				}
			} else
			{
				// values are indexed by attr names
				double recValue = Double.parseDouble(allAttr.getString(attrName));
				boolean checkRes = Utils.checkQCForOverlapWithValue(recValue, qc);
				
				// if not satisfies, then group not satisfied
				if(!checkRes)
				{
					satisfiesGroup = false;
					break;
				}
			}
		}
		return satisfiesGroup;
	}
	
	/**
	 * Takes all the context attributes, new and the old values, 
	 * checks if the group query is still satisfied or not.
	 * @return
	 * @throws JSONException 
	 * @throws NumberFormatException 
	 */
	public static boolean groupMemberCheck(Map<String, Object> hyperdexMap, String updateAttrName
			, double attrVal, String groupQuery)
			throws JSONException
	{
		boolean satisfiesGroup = true;
		Vector<QueryComponent> groupQC = QueryParser.parseQuery(groupQuery);
		
		for(int j=0;j<groupQC.size();j++)
		{
			QueryComponent qc = groupQC.get(j);
			String attrName = qc.getAttributeName();
			
			// if this is the case, don't use the given val
			if( attrName.equals(updateAttrName) )
			{
				// values are indexed by attr names
				double recValue = attrVal;
				boolean checkRes = Utils.checkQCForOverlapWithValue(recValue, qc);
				
				// if not satisfies, then group not satisfied
				if(!checkRes)
				{
					satisfiesGroup = false;
					break;
				}
			} else
			{
				if(hyperdexMap != null)
				{
					// values are indexed by attr names
					Object retObj = hyperdexMap.get(attrName);
					if( retObj != null )
					{
						double recValue =  (Double)retObj;
						boolean checkRes = Utils.checkQCForOverlapWithValue(recValue, qc);
						
						// if not satisfies, then group not satisfied
						if(!checkRes)
						{
							satisfiesGroup = false;
							break;
						}
					}
				}
			}
		}
		return satisfiesGroup;
	}
	
	public static List<String> getAttributesInQuery(String query)
	{
		Vector<QueryComponent> groupQC = QueryParser.parseQuery(query);
		List<String> attrList = new LinkedList<String>();
		
		for(int i=0;i<groupQC.size();i++)
		{
			attrList.add(groupQC.get(i).getAttributeName());
		}
		return attrList;
	}
	
	public static String getSHA1( String stringToHash )
	{
	   //Hashing.consistentHash(input, buckets);
	   MessageDigest md=null;
	   try 
	   {
		   md = MessageDigest.getInstance("SHA-256");
	   } catch (NoSuchAlgorithmException e) {
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
       
       LOGGER.fine("Hex format : " + sb.toString());
       return sb.toString();
	}
	
	/**
	 * returns true if the given address belongs to this machine
	 * @return
	 */
	public static boolean isMyMachineAddress(InetAddress givenAddress)
	{
		Vector<InetAddress> currIPs = getActiveInterfaceInetAddresses();
		for( int i=0;i<currIPs.size();i++ )
		{
			System.out.println("givenAddress "+givenAddress+
					" currIPs.get(i) "+currIPs.get(i));
			if(currIPs.get(i).getHostAddress().equals(givenAddress.getHostAddress()))
			{
				return true;
			}
		}
		return false;
	}
	
	//public static void sendUDP( String mesg )
	//{
		/*try
		{
			DatagramSocket client_socket = new DatagramSocket();
			
			byte[] send_data = new byte[1024];	
			//BufferedReader infromuser = 
	        //                new BufferedReader(new InputStreamReader(System.in));
			
			InetAddress IPAddress =  InetAddress.getByName(Utils.UDPServer);
			
			//String data = infromuser.readLine();
			send_data = mesg.getBytes();
			DatagramPacket send_packet = new DatagramPacket(send_data,
	                                                        send_data.length, 
	                                                        IPAddress, UDPPort);
			client_socket.send(send_packet);
			
			client_socket.close();
		} catch(Exception ex)
		{
			ex.printStackTrace();
		}*/
	//}
	
	public static void main(String[] args)
	{
		//  open up standard input
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String query = null;
		//  read the username from the command-line; need to use try/catch with the
		//  readLine() method
		
		try
		{
			query = br.readLine();
			
			System.out.println("Query entered "+query);
			
			System.out.println("Query hash "+getSHA1(query));
		} catch (IOException e1)
		{
			e1.printStackTrace();
		}
	}
	
}