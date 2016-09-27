package edu.umass.cs.contextservice.client.anonymizedID.test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import edu.umass.cs.contextservice.client.anonymizedID.HyperspaceBasedAnonymizedIDCreator;
import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;

public class LargeACLSizeWorkload
{
	public static final String GUID_PREFIX			= "GUID_PREFIX";
	public static int numAttrs;
	public static int numCircles;
	public static int numGUIDsPerCircle;
	public static int numCirclesPerACL;
	
	private final HashMap<Integer, List<GuidEntry>> circleMap;
	private final HashMap<String, List<ACLEntry>> aclMap;
	
	private final Random aclRand;
	
	public LargeACLSizeWorkload()
	{
		circleMap = new HashMap<Integer, List<GuidEntry>>();
		aclMap = new HashMap<String, List<ACLEntry>>();
		aclRand = new Random();
	}
	
	private void generateCircles()
				throws NoSuchAlgorithmException, EncryptionException
	{
		KeyPairGenerator kpg;
		kpg = KeyPairGenerator.getInstance("RSA");
		
		int count = 0;
		for( int i=0; i<numCircles; i++ )
		{
			List<GuidEntry> circleGUIDList = new LinkedList<GuidEntry>();
			
			for( int j=0;  j < numGUIDsPerCircle; j++ )
			{
				KeyPair kp = kpg.genKeyPair();
				PublicKey publicKey = kp.getPublic();
				PrivateKey privateKey = kp.getPrivate();
				byte[] publicKeyByteArray = publicKey.getEncoded();
				byte[] privateKeyByteArray = privateKey.getEncoded();
				
				String guid = Utils.convertPublicKeyToGUIDString(publicKeyByteArray);
				
				count++;
				GuidEntry currGUID = new GuidEntry(GUID_PREFIX+count, guid, 
						publicKey, privateKey);
				
				circleGUIDList.add(currGUID);
			}
			
			//System.out.println( "generateCircles "+i +" "+circleGUIDList.size() );
			circleMap.put(i, circleGUIDList);
		}
	}
	
	
	private void generateACLs()
	{
		for( int i=0; i<numAttrs; i++ )
		{
			String attrName = "attr"+i;
			List<ACLEntry> aclList = new LinkedList<ACLEntry>();
			
			for( int j=0; j < numCirclesPerACL; j++ )
			{
				HashMap<Integer, Boolean> currCircles 
						= getDistinctCircles(numCirclesPerACL);
				
				Iterator<Integer> circleIdIter 
						= currCircles.keySet().iterator();
				
				while( circleIdIter.hasNext() )
				{
					int circleId = circleIdIter.next();
					
					//System.out.println("circleId "+circleId);
					
					List<GuidEntry> circleList = circleMap.get(circleId);
					
					for( int k=0; k < circleList.size(); k++ )
					{
						
						GuidEntry guidEntry = circleList.get(k);
						
						ACLEntry aclEntry 
							= new ACLEntry(guidEntry.getGuid(), guidEntry.getPublicKey());
						
						aclList.add(aclEntry);
					}
				}
			}	
			aclMap.put(attrName, aclList);
		}
	}
	
	private HashMap<String, List<ACLEntry>> getACLMap()
	{
		return this.aclMap;
	}
	
	
	private HashMap<Integer, Boolean> getDistinctCircles(int numberOfCirlcesToGet)
	{
		HashMap<Integer, Boolean> circleIDMap = new HashMap<Integer, Boolean>();
		
		while( numberOfCirlcesToGet != circleIDMap.size() )
		{
			int circleIndex = aclRand.nextInt(numCircles);
			circleIDMap.put(circleIndex, true);
		}
		return circleIDMap;
	}
	
	
	public static void main( String[] args )
					throws EncryptionException, NoSuchAlgorithmException
	{
		numAttrs 		 	= Integer.parseInt(args[0]);
		numCircles 		 	= Integer.parseInt(args[1]);
		numGUIDsPerCircle 	= Integer.parseInt(args[2]);
		numCirclesPerACL 	= Integer.parseInt(args[3]);
		
		LargeACLSizeWorkload obj = new LargeACLSizeWorkload();
		
		obj.generateCircles();
		obj.generateACLs();
		
		
		KeyPairGenerator kpg;
		kpg = KeyPairGenerator.getInstance("RSA");
		
		KeyPair kp = kpg.genKeyPair();
		PublicKey publicKey = kp.getPublic();
		PrivateKey privateKey = kp.getPrivate();
		byte[] publicKeyByteArray = publicKey.getEncoded();
		//byte[] privateKeyByteArray = privateKey.getEncoded();
		
		String guid = Utils.convertPublicKeyToGUIDString(publicKeyByteArray);
		
		GuidEntry currGUID = new GuidEntry("myGUID", guid, 
				publicKey, privateKey);
		
		
		HyperspaceBasedAnonymizedIDCreator hyperspaceBasedIDCreator = 
				new HyperspaceBasedAnonymizedIDCreator();
		
		List<AnonymizedIDEntry> anonymizedIDList = 
					hyperspaceBasedIDCreator.computeAnonymizedIDs(currGUID, obj.aclMap);
		
		System.out.println("Number of anonymizedIDs created "+anonymizedIDList.size());
	}
}

/*public static String getGUID( String stringToHash )
{
	MessageDigest md=null;
	try
	{
		md = MessageDigest.getInstance("SHA-256");
	}
	catch ( NoSuchAlgorithmException e )
	{
		e.printStackTrace();
	}
   
	md.update(stringToHash.getBytes());
	byte byteData[] = md.digest();

	//convert the byte to hex format method 1
	StringBuffer sb = new StringBuffer();
	for (int i = 0; i < byteData.length; i++) 
	{
		sb.append(Integer.toString
    				((byteData[i] & 0xff) + 0x100, 16).substring(1));
	}
	String returnGUID = sb.toString();
	return returnGUID.substring(0, 40);
}*/