package edu.umass.cs.contextservice.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Logger;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.logging.ContextServiceLogger;

/**
 * 
 * Class specifies the common utility methods like SHA1 hashing etc.
 * @author ayadav
 */

public class Utils
{
	public static final int GUID_SIZE				= 20; // 20 bytes
	private static final Logger LOGGER 				= ContextServiceLogger.getLogger();
	
	
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
		  //ContextServiceLogger.getLogger().fine(" numPredicates "+numPredicates);
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
					  //ContextServiceLogger.getLogger().fine("Key "+curString);
					  conjuctionMap.put(curString, 1);
				  } 
				  else
				  {
					  //ContextServiceLogger.getLogger().fine("Key++ "+curString+", "+count+1);
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
		  //ContextServiceLogger.getLogger().fine("conjuctionMap "+conjuctionMap);
		  return resultJSON;
	  }
	  
	  
	  public static JSONArray doDisjuction(LinkedList<LinkedList<String>> elements)
	  {
		  LinkedList<String> result = new LinkedList<String>();
		  //ContextServiceLogger.getLogger().fine(" numPredicates "+numPredicates);
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
					  //ContextServiceLogger.getLogger().fine("Key "+curString);
					  disjunctionMap.put(curString, 1);
				  } 
				  else
				  {
					  //ContextServiceLogger.getLogger().fine("Key++ "+curString+", "+count+1);
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
		  //ContextServiceLogger.getLogger().fine("conjuctionMap "+conjuctionMap);
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
	
	public static String convertPublicKeyToGUIDString(byte[] publicKeyByteArray)
	{
		//Hashing.consistentHash(input, buckets);
		MessageDigest md=null;
		try 
		{
			md = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) 
		{
			e.printStackTrace();
		}
		
		md.update(publicKeyByteArray);
		byte byteData[] = md.digest();
 
		//convert the byte to hex format method 1
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < byteData.length; i++) 
		{
			sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
		}
		
		//LOGGER.fine("Hex format : " + sb.toString());
		return sb.toString().substring(0, GUID_SIZE*2);
	}
	
	
	public static byte[] convertPublicKeyToGUIDByteArray(byte[] publicKeyByteArray)
	{
		MessageDigest md=null;
		try 
		{
			md = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) 
		{
			e.printStackTrace();
		}
		
		md.update(publicKeyByteArray);
		byte byteData[] = md.digest();
		
		// 20 byte guid
		byte[] guid = new byte[GUID_SIZE];
		
		for( int i=0; i<GUID_SIZE; i++ )
		{
			guid[i] = byteData[i];
		}
		
		return guid;
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
			ContextServiceLogger.getLogger().fine( "givenAddress "+givenAddress+
					" currIPs.get(i) "+currIPs.get(i) );
			if( currIPs.get(i).getHostAddress().equals(givenAddress.getHostAddress()) )
			{
				return true;
			}
		}
		return false;
	}
	
	public static double roundTo(double value, int places) 
	{
		if ( places < 0 || places > 20 )
		{
			throw new IllegalArgumentException();
	    }
		
		double factor = Math.pow(10.0, places);
	    value = value * factor;
	    double tmp = Math.round(value);
	    return tmp / factor;
	}
	
	/**
	 * does public key encryption and returns the byte[]
	 * @param publicKey
	 * @param plainTextByteArray
	 * @return
	 * @throws NoSuchAlgorithmException 
	 * @throws InvalidKeySpecException 
	 * @throws NoSuchPaddingException 
	 * @throws InvalidKeyException 
	 * @throws BadPaddingException 
	 * @throws IllegalBlockSizeException 
	 */
	public static byte[] doPublicKeyEncryption(byte[] publicKeyBytes, byte[] plainTextByteArray) 
			throws NoSuchAlgorithmException, InvalidKeySpecException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException
	{
//		byte[] privateKeyBytes;
//		byte[] publicKeyBytes;
//		KeyFactory kf = KeyFactory.getInstance("RSA"); // or "EC" or whatever
//		PrivateKey private = kf.generatePrivate(new PKCS8EncodedKeySpec(privateKeyBytes));
//		PublicKey public = kf.generatePublic(new X509EncodedKeySpec(publicKeyBytes));
		
		KeyFactory kf = KeyFactory.getInstance("RSA"); // or "EC" or whatever
		//PrivateKey private = kf.generatePrivate(new PKCS8EncodedKeySpec(privateKeyBytes));
		PublicKey publicKey = kf.generatePublic(new X509EncodedKeySpec(publicKeyBytes));
		
		byte[] cipherText = null;
		
		// get an RSA cipher object and print the provider
		Cipher cipher = Cipher.getInstance("RSA");
		// encrypt the plain text using the public key
		cipher.init(Cipher.ENCRYPT_MODE, publicKey);
		cipherText = cipher.doFinal(plainTextByteArray);
	    return cipherText;
	}
	
	
	/**
	 * does private key decryption and returns the byte[]
	 * @param publicKey
	 * @param plainTextByteArray
	 * @return
	 * @throws NoSuchAlgorithmException 
	 * @throws InvalidKeySpecException 
	 * @throws NoSuchPaddingException 
	 * @throws InvalidKeyException 
	 * @throws BadPaddingException 
	 * @throws IllegalBlockSizeException 
	 */
	public static byte[] doPrivateKeyDecryption(byte[] privateKeyBytes, byte[] encryptedTextByteArray) 
			throws NoSuchAlgorithmException, InvalidKeySpecException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException
	{
//		byte[] privateKeyBytes;
//		byte[] publicKeyBytes;
//		KeyFactory kf = KeyFactory.getInstance("RSA"); // or "EC" or whatever
//		PrivateKey private = kf.generatePrivate(new PKCS8EncodedKeySpec(privateKeyBytes));
//		PublicKey public = kf.generatePublic(new X509EncodedKeySpec(publicKeyBytes));
		
		KeyFactory kf = KeyFactory.getInstance("RSA"); // or "EC" or whatever
		PrivateKey privateKey = kf.generatePrivate(new PKCS8EncodedKeySpec(privateKeyBytes));
		//PublicKey publicKey = kf.generatePublic(new X509EncodedKeySpec(publicKeyBytes));
		
		byte[] plainText = null;
		
		// get an RSA cipher object and print the provider
		Cipher cipher = Cipher.getInstance("RSA");
		// encrypt the plain text using the public key
		cipher.init(Cipher.DECRYPT_MODE, privateKey);
		plainText = cipher.doFinal(encryptedTextByteArray);
		
	    return plainText;
	}
	
	//public static byte[] 
	
	
	public static void main( String[] args )
	{
		//open up standard input
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String query = null;
		//  read the username from the command-line; need to use try/catch with the
		//  readLine() method
		
		try
		{
			query = br.readLine();
			
			ContextServiceLogger.getLogger().fine("Query entered "+query);
			
			ContextServiceLogger.getLogger().fine("Query hash "+getSHA1(query));
		} catch (IOException e1)
		{
			e1.printStackTrace();
		}
	}
	
	
	/**
	 * checks if the input attr value lies within the range
	 * @return
	 */
	/*public static boolean checkQCForOverlapWithValue(double attrValue, QueryComponent qc)
	{
		if( (qc.getLeftValue() <= attrValue) && (qc.getRightValue() > attrValue) )
		{
			return true;
		}
		else
		{
			return false;
		}
	}*/
	
	/**
	 * Takes all the context attributes, new and the old values, 
	 * checks if the group query is still satisfied or not.
	 * @return
	 * @throws JSONException 
	 * @throws NumberFormatException 
	 */
	/*public static boolean groupMemberCheck(JSONObject allAttr, String updateAttrName
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
	}*/
	
	/**
	 * Takes all the context attributes, new and the old values, 
	 * checks if the group query is still satisfied or not.
	 * @return
	 * @throws JSONException 
	 * @throws NumberFormatException 
	 */
	/*public static boolean groupMemberCheck(Map<String, Object> hyperdexMap, String updateAttrName
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
	}*/
	
	/*public static List<String> getAttributesInQuery(String query)
	{
		Vector<QueryComponent> groupQC = QueryParser.parseQuery(query);
		List<String> attrList = new LinkedList<String>();
		
		for(int i=0;i<groupQC.size();i++)
		{
			attrList.add(groupQC.get(i).getAttributeName());
		}
		return attrList;
	}*/
}