package edu.umass.cs.contextservice.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Enumeration;
import java.util.Random;
import java.util.Vector;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.hash.Hashing;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;

/**
 * 
 * Class specifies the common utility methods like SHA1 hashing etc.
 * @author ayadav
 */

public class Utils
{
	public static final int GUID_SIZE				= 20; // 20 bytes
	
	public static Vector<String> getActiveInterfaceStringAddresses()
	{
		Vector<String> CurrentInterfaceIPs = new Vector<String>();
	    try
	    {
	    	for (Enumeration<NetworkInterface> en 
	    			= NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();)
	    	{
	    		NetworkInterface intf = en.nextElement();
	    		for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); 
	    				enumIpAddr.hasMoreElements();)
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
			  for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); 
					  en.hasMoreElements();)
			  {
				  NetworkInterface intf = en.nextElement();
				  for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); 
						  enumIpAddr.hasMoreElements();)
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
	   * FIXME: check the time. It could be  crtical path
	   * @param stringToHash
	   * @return
	   */
	  public static String getSHA1( String stringToHash )
	  {
//		  if(false)
//		  {
//			  MessageDigest md=null;
//			  try 
//			  {
//				  md = MessageDigest.getInstance("SHA-256");
//			  } catch (NoSuchAlgorithmException e) 
//			  {
//				  e.printStackTrace();
//			  }
//			  
//			  md.update(stringToHash.getBytes());
//			  
//			  byte[] byteData = md.digest();
//			  
//			  char[] byteRep = Hex.encodeHex(byteData);
//			  
//			  return new String(byteRep);
//		  }
		  //else
		  {
			  Random rand = new Random(stringToHash.hashCode());
			  byte[] guidByte = new byte[20];
			  rand.nextBytes(guidByte);
			  
			  char[] byteRep = Hex.encodeHex(guidByte);
			  
			  return new String(byteRep);
		  }
	  }
	
	/**
	 * FIXME: check the time, although not in critical path.
	 * @param publicKeyByteArray
	 * @return
	 */
	public static String convertPublicKeyToGUIDString(byte[] publicKeyByteArray)
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
			byte[] byteData = md.digest();
	 
			char[] byteRep = Hex.encodeHex(byteData);
			
			//LOGGER.fine("Hex format : " + sb.toString());
		return new String(byteRep).substring(0, GUID_SIZE*2);
	}
	
	/**
	 * FIXME: check the time, although this is not in critical path
	 * @param publicKeyByteArray
	 * @return
	 */
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
	

	
	/**
	 * FIXME: check the time
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
		if(ContextServiceConfig.NO_ENCRYPTION)
			return plainTextByteArray;
		
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
		
//		System.out.println("doPublicKeyEncryption time "+(end-start)+" plainTextByteArray "
//						+plainTextByteArray.length);
	    return cipherText;
	}
	
	
	/**
	 * FIXME: check the time
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
		if(ContextServiceConfig.NO_ENCRYPTION)
			return encryptedTextByteArray;
		
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
		
//		System.out.println("doPrivateKeyDecryption time "+(end-start));
	    return plainText;
	}
	
	/**
	 * FIXME: check the time
	 * @param symmetricKey
	 * @param plainTextByteArray
	 * @return
	 * @throws NoSuchAlgorithmException
	 * @throws NoSuchPaddingException
	 * @throws InvalidKeyException
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException
	 */
	public static byte[] doSymmetricEncryption(byte[] symmetricKey, byte[] plainTextByteArray) 
			throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, 
			IllegalBlockSizeException, BadPaddingException
	{
		SecretKey keyObj = new SecretKeySpec(symmetricKey, 0, symmetricKey.length, 
				ContextServiceConfig.SymmetricEncAlgorithm);
		
		//TODO: need to check how much overhead is creating a cipher everytime.
		// GNS has a list of pre created ciphers which are used in a mutually exclusive manner.
		// If Cipher.getInstance has overhead than CNS code also needs to have a list of pre created 
		// ciphers.
		Cipher c = Cipher.getInstance(ContextServiceConfig.SymmetricEncAlgorithm);
		c.init(Cipher.ENCRYPT_MODE, keyObj);
		
		return c.doFinal(plainTextByteArray);
	}
	
	/**
	 * FIXME: Check the time this method takes. 
	 * Should mathc with openssl speed tes 
	 * @param symmetricKey
	 * @param encryptedByteArray
	 * @return
	 * @throws NoSuchAlgorithmException
	 * @throws NoSuchPaddingException
	 * @throws InvalidKeyException
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException
	 */
	public static byte[] doSymmetricDecryption(byte[] symmetricKey, byte[] encryptedByteArray) 
			throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, 
			IllegalBlockSizeException, BadPaddingException
	{	
		SecretKey keyObj = new SecretKeySpec(symmetricKey, 0, symmetricKey.length, 
				ContextServiceConfig.SymmetricEncAlgorithm);
		
		Cipher c = Cipher.getInstance(ContextServiceConfig.SymmetricEncAlgorithm);
		c.init(Cipher.DECRYPT_MODE, keyObj);
		return c.doFinal(encryptedByteArray);
	}
	
	/**
	 * FIXME: check the time this takes. SHould be less than 1 ms
	 * @param stringToHash
	 * @param numItems
	 * @return
	 */
	public static int consistentHashAString( String stringToHash , 
			int numItems )
	{
		stringToHash = stringToHash.toLowerCase();
		int mapIndex = Hashing.consistentHash(stringToHash.hashCode(), numItems);
		return mapIndex;
	}
	
	/**
	 * Checking this functions time is important. This is in critical path
	 * @param a
	 * @return
	 */
	public static String byteArrayToHex(byte[] a)
	{
		char[] hexBytes = Hex.encodeHex(a);
		return new String(hexBytes);
	}
	
	public static byte[] hexStringToByteArray(String s) 
	{
		byte[] byteArray = null;		
		try
		{
			byteArray =	Hex.decodeHex(s.toCharArray());
		} 
		catch (DecoderException e) 
		{
			e.printStackTrace();
		}
		return byteArray;
	}
	
	//FIXME: test time taken by each method here
	public static void main( String[] args ) throws JSONException, NoSuchAlgorithmException, InvalidKeyException, 
											InvalidKeySpecException, NoSuchPaddingException, 
											IllegalBlockSizeException, BadPaddingException
	{
		//String str = "abcdefghijklmnopqrstuvwxyz"; 
		long start = System.currentTimeMillis();
		//byte[] newByte = new byte[20];
		Random rand = new Random();
//		for(int i=0;i<1000; i++)
//		{
//			//String hashVal = getSHA1( str );
//			//rand.nextBytes(newByte);
//		}
		long end = System.currentTimeMillis();
		System.out.println("Time for for SHA1 hash "+(end-start));
		// json and byte[] hex conv check
		int numGuids = 10000;
		rand = new Random(0);
		JSONArray resultJSON = new JSONArray();
		
		for(int i=0; i<numGuids; i++)
		{
			byte[] guidBytes = new byte[20];
			rand.nextBytes(guidBytes);
		}
		
		// time to convert to json tostring
		String jsonString = resultJSON.toString();		
		
		resultJSON = new JSONArray(jsonString);
//		System.out.println("JSON fromString time taken "
//				+(System.currentTimeMillis() - start));
		
		KeyPairGenerator kpg = KeyPairGenerator.getInstance( "RSA" );
		KeyPair kp0 = kpg.generateKeyPair();
		PublicKey publicKey0 = kp0.getPublic();
		byte[] publicKeyBytes = publicKey0.getEncoded();
		
		byte[] guidBytes = new byte[20];
		rand.nextBytes(guidBytes);
		
		long s1 = System.currentTimeMillis();
		byte[] encrypted1 = Utils.doPublicKeyEncryption(publicKeyBytes, guidBytes);
		long e1 = System.currentTimeMillis();
		
		
		s1 = System.currentTimeMillis();
		encrypted1 = Utils.doPublicKeyEncryption(publicKeyBytes, guidBytes);
		e1 = System.currentTimeMillis();
		
		System.out.println("Encryption time "+(e1-s1));
		
		guidBytes = new byte[20];
		rand.nextBytes(guidBytes);
		
		s1 = System.currentTimeMillis();
		byte[] encrypted2 = Utils.doPublicKeyEncryption(publicKeyBytes, guidBytes);
		e1 = System.currentTimeMillis();
		System.out.println("Encryption time "+(e1-s1));
		
		s1 = System.currentTimeMillis();
		String encString1 = Utils.byteArrayToHex(encrypted1);
		e1 = System.currentTimeMillis();
		System.out.println("Encryption to string time "+(e1-s1));
		
		s1 = System.currentTimeMillis();
		String encString2 = Utils.byteArrayToHex(encrypted2);
		e1 = System.currentTimeMillis();
		
		System.out.println("Encryption to string time "+(e1-s1));
		
		JSONArray encArray = new JSONArray();
		s1 = System.currentTimeMillis();
		encArray.put(encString1);
		encArray.put(encString2);
		e1 = System.currentTimeMillis();
		
		System.out.println("JSONArray insert time "+(e1-s1));
		
		s1 = System.currentTimeMillis();
		String jsonArrString = encArray.toString();
		e1 = System.currentTimeMillis();
		
		System.out.println("JSONArray to string time "+(e1-s1));
		
		
		s1 = System.currentTimeMillis();
		encArray = new JSONArray(jsonArrString);
		e1 = System.currentTimeMillis();
		
		System.out.println("JSONArray from string time "+(e1-s1));
	}
}