package edu.umass.cs.contextservice.client.csprivacytransform;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;

import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.dataformat.SearchReplyGUIDRepresentationJSON;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gnsclient.client.util.GuidEntry;

/**
 * Implements the parallel decryption of search reply.
 * @author adipc
 *
 */
public class ParallelSearchReplyASymmetricKeyDecryption 
{
	private final GuidEntry myGuid;
	private final List<CSSearchReplyTransformedMessage> csTransformedList;
	private final JSONArray replyArray;
	//private final ExecutorService execService;
	
	private long numFinished 				= 0;
	private final Object lock 				= new Object();
	
	private final HashMap<String, Boolean> replyMap;
	
	private int totalDecryptionsOverall		= 0;
	
	public ParallelSearchReplyASymmetricKeyDecryption(GuidEntry myGuid , 
			List<CSSearchReplyTransformedMessage> csTransformedList
			, JSONArray replyArray, ExecutorService execService)
	{
		this.myGuid = myGuid;
		this.csTransformedList = csTransformedList;
		this.replyArray = replyArray;
		//this.execService = execService;
		replyMap = null;
	}
	
	public ParallelSearchReplyASymmetricKeyDecryption(GuidEntry myGuid , 
			List<CSSearchReplyTransformedMessage> csTransformedList
			, HashMap<String, Boolean> replyMap, ExecutorService execService)
	{
		this.myGuid = myGuid;
		this.csTransformedList = csTransformedList;
		this.replyArray = null;
		//this.execService = execService;
		this.replyMap = replyMap;
	}
	
	/**
	 * Blocking call, does the decryption of the search 
	 * reply, in csTransformedList, and returns real GUIDs 
	 * in replyArray
	 */
	public void doDecryption()
	{
		for(int i=0; i<csTransformedList.size();i++)
		{
			CSSearchReplyTransformedMessage csSearchRepMessage 
											= csTransformedList.get(i);
			
			SearchReplyGUIDRepresentationJSON searchReplyJSON 
								= csSearchRepMessage.getSearchGUIDObj();
			
			if(searchReplyJSON.getAnonymizedIDToGuidMapping() != null)
			{	
				SearchReplyDecryptionThread searchRepThread = new SearchReplyDecryptionThread
						( myGuid, csSearchRepMessage.getSearchGUIDObj() );
				
				// just doing it sequentially, then we can check how much time it takes. 
				// as we know the decryption time , and number of decryptions.
				// times should match here.
				searchRepThread.run();
//				execService.execute( new SearchReplyDecryptionThread
//						( myGuid, csSearchRepMessage.getSearchGUIDObj() ) );
				
			}
			else
			{
				synchronized(lock)
				{
					// no privacy case. GUID is the ID here.
					numFinished++;
					if(replyArray != null)
						replyArray.put( searchReplyJSON.getID() );
					else if(replyMap != null)
					{
						replyMap.put(searchReplyJSON.getID(), true);
					}
					
					if( numFinished == csTransformedList.size() )
					{
						lock.notify();
					}
				}
			}
		}
		
		synchronized( lock )
		{
			while( numFinished != csTransformedList.size() )
			{
				try
				{
					lock.wait();
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	public int getTotalDecryptionsOverall()
	{
		return this.totalDecryptionsOverall;
	}
	
	/**
	 * Decrypts the anonymized ID in the search reply.
	 * @author adipc
	 */
	private class SearchReplyDecryptionThread implements Runnable
	{
		private final GuidEntry myGUIDInfo;
		private final SearchReplyGUIDRepresentationJSON seachReply;
		
		private int totalDecryptionsThread = 0;
		
		public SearchReplyDecryptionThread( GuidEntry myGUIDInfo , 
				SearchReplyGUIDRepresentationJSON seachReply )
		{
			this.myGUIDInfo = myGUIDInfo;
			this.seachReply = seachReply;
		}
		
		@Override
		public void run()
		{
			byte[] plainTextBytes = decryptRealIDFromSearchRep( myGUIDInfo, seachReply );
			
			if( plainTextBytes != null )
			{
				synchronized(lock)
				{
					numFinished++;
					totalDecryptionsOverall = totalDecryptionsOverall + totalDecryptionsThread;
					if( replyArray != null )
					{
						replyArray.put( Utils.byteArrayToHex(plainTextBytes) );
					}
					else if( replyMap != null )
					{
						replyMap.put(Utils.byteArrayToHex(plainTextBytes), true);
					}
						
						
					if( numFinished == csTransformedList.size() )
					{
						lock.notify();
					}
				}
			}
			else
			{
				synchronized(lock)
				{
					numFinished++;
					totalDecryptionsOverall = totalDecryptionsOverall + totalDecryptionsThread;
					if( numFinished == csTransformedList.size() )
					{
						lock.notify();
					}
				}
			}
		}
		
		
		/**
		 * Decrypts the real ID from search reply using realID mapping info.
		 * Returns null if it cannot be decrypted.
		 * @param myGUIDInfo
		 * @param encryptedRealJsonArray
		 * @return
		 * @throws JSONException 
		 */
		private byte[] decryptRealIDFromSearchRep( GuidEntry myGUIDInfo, 
				SearchReplyGUIDRepresentationJSON seachReply ) 
		{
			byte[] privateKey = myGUIDInfo.getPrivateKey().getEncoded();
			byte[] plainText = null;
			
			JSONArray anonymizedIDToGuidMapping 
								= seachReply.getAnonymizedIDToGuidMapping();
			
			if( anonymizedIDToGuidMapping != null )
			{
//				System.out.println("ID "+seachReply.getID()+" realIDMappingInfo JSONArray "
//						+ anonymizedIDToGuidMapping.length() + " "+anonymizedIDToGuidMapping);
				
				String myGuidString = myGUIDInfo.getGuid();
				
				int indexToCheck = Utils.consistentHashAString(myGuidString, 
												anonymizedIDToGuidMapping.length());
				int numChecked = 0;
				
				while(numChecked < anonymizedIDToGuidMapping.length())
				{
					try
					{
//						System.out.println("indexToCheck "+indexToCheck
//								+" anonymizedIDToGuidMapping "
//								+anonymizedIDToGuidMapping.length());
						
						byte[] encryptedElement =  Utils.hexStringToByteArray(
								anonymizedIDToGuidMapping.getString(indexToCheck));
						totalDecryptionsThread++;
						
						plainText = Utils.doPrivateKeyDecryption(privateKey, encryptedElement);
						
						// non exception, just break;
						break;
					}
					catch(javax.crypto.BadPaddingException wrongKeyException)
					{
						// just catching this one, as this one results when wrong key is used 
						// to decrypt.
					} catch ( InvalidKeyException | NoSuchAlgorithmException
							| InvalidKeySpecException | NoSuchPaddingException
							| IllegalBlockSizeException | JSONException
							e )
					{
						e.printStackTrace();
					}
					numChecked++;
					indexToCheck++;
					indexToCheck = indexToCheck%anonymizedIDToGuidMapping.length();
				}
			}
			
			if(plainText != null)
			{
				ContextServiceLogger.getLogger().fine("Anonymized ID "+seachReply.getID()
										+ "realID "+Utils.byteArrayToHex(plainText) );
			}
			return plainText;
		}
	}
}