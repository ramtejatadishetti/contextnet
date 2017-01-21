package edu.umass.cs.contextservice.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.anonymizedID.AnonymizedIDCreationInterface;
import edu.umass.cs.contextservice.client.anonymizedID.HyperspaceBasedASymmetricKeyAnonymizedIDCreator;
import edu.umass.cs.contextservice.client.anonymizedID.HyperspaceBasedSymmetricKeyAnonymizedIDCreator;
import edu.umass.cs.contextservice.client.callback.implementations.BlockingCallBack;
import edu.umass.cs.contextservice.client.callback.implementations.BlockingSearchReply;
import edu.umass.cs.contextservice.client.callback.implementations.BlockingUpdateReply;
import edu.umass.cs.contextservice.client.callback.implementations.PrivacyCallBack;
import edu.umass.cs.contextservice.client.callback.implementations.PrivacyUpdateReply;
import edu.umass.cs.contextservice.client.callback.implementations.NoopCallBack;
import edu.umass.cs.contextservice.client.callback.implementations.NoopSearchReply;
import edu.umass.cs.contextservice.client.callback.implementations.NoopUpdateReply;
import edu.umass.cs.contextservice.client.callback.interfaces.CallBackInterface;
import edu.umass.cs.contextservice.client.callback.interfaces.SearchReplyInterface;
import edu.umass.cs.contextservice.client.callback.interfaces.UpdateReplyInterface;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.client.csprivacytransform.CSPrivacyTransformInterface;
import edu.umass.cs.contextservice.client.csprivacytransform.CSSearchReplyTransformedMessage;
import edu.umass.cs.contextservice.client.csprivacytransform.CSUpdateTransformedMessage;
import edu.umass.cs.contextservice.client.csprivacytransform.HyperspaceBasedASymmetricKeyCSTransform;
import edu.umass.cs.contextservice.client.csprivacytransform.HyperspaceBasedSymmetricKeyCSTransform;
import edu.umass.cs.contextservice.client.gnsprivacytransform.EncryptionBasedGNSPrivacyTransform;
import edu.umass.cs.contextservice.client.gnsprivacytransform.GNSPrivacyTransformInterface;
import edu.umass.cs.contextservice.client.gnsprivacytransform.GNSTransformedMessage;
import edu.umass.cs.contextservice.client.profiler.ClientProfilerStatClass;
import edu.umass.cs.contextservice.client.storage.GetStorage;
import edu.umass.cs.contextservice.client.storage.SearchQueryStorage;
import edu.umass.cs.contextservice.client.storage.UpdateStorage;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.config.ContextServiceConfig.PrivacySchemes;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.ClientConfigReply;
import edu.umass.cs.contextservice.messages.ClientConfigRequest;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.GetMessage;
import edu.umass.cs.contextservice.messages.GetReplyMessage;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;
import edu.umass.cs.contextservice.messages.RefreshTrigger;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNSReply;
import edu.umass.cs.contextservice.messages.dataformat.SearchReplyGUIDRepresentationJSON;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.nio.nioutils.NIOHeader;

/**
 * ContextService client.
 * It is used to send and recv replies from context service.
 * It knows context service node addresses from a file in the conf folder.
 * It is thread safe, means same client can be used by multiple threads without any 
 * synchronization problems.
 * @author adipc
 * @param <Integer>
 */
public class ContextServiceClient extends AbstractContextServiceClient 
				implements ContextClientInterfaceWithPrivacy, 
				ContextServiceClientInterfaceWithoutPrivacy
{
	// if experiment mode is true then triggers are not stored in a queue.
	public static boolean EXPERIMENT_MODE								= true;
	
	// enables the profiler 
	private static final boolean PROFILER_ENABLE						= false;
	
	public static final String SYMMETRIC_KEY_EXCHANGE_FIELD_NAME		= "SYMMETRIC_KEY_EXCHANGE_FIELD";
	
	public static final String GNSCLIENT_CONF_FILE_NAME					= "gnsclient.contextservice.properties";
	
	
	private Queue<JSONObject> refreshTriggerQueue;
	
	private final Object refreshTriggerClientWaitLock 					= new Object();
	
	private GNSClient gnsClient;
	
	// asymmetric key id creation.
	private AnonymizedIDCreationInterface asymmetricAnonymizedIDCreation;
	
	// symmetric key id creation.
	private AnonymizedIDCreationInterface symmetricAnonymizedIDCreation;
	
	//gns transform
	private GNSPrivacyTransformInterface gnsPrivacyTransform;
	
	// asymmetric key privacy transform
	private CSPrivacyTransformInterface asymmetricCSPrivacyTransform;
	
	// symmetric key privacy transform.
	private CSPrivacyTransformInterface symmetricCSPrivacyTransform;
	
	
	private PrivacyCallBack privacyCallBack;
	
	private BlockingCallBack blockingCallBack;
	
	private long blockingReqID 											= 0;
	private final Object blockingReqIDLock 								= new Object();
	
	// used to get stats for experiment.
	private double sumNumAnonymizedIdsUpdated							= 0.0;
	private long totalPrivacyUpdateReqs									= 0;
	private Object printLocks											= new Object();
	
	private double sumAddedGroupGUIDsOnUpdate							= 0.0;
	private double sumRemovedGroupGUIDsOnUpdate							= 0.0;
	private long numTriggers											= 1;
	
	private long lastPrintTime;
	
	private final long startTime										= System.currentTimeMillis();
	
	private final ExecutorService execService;
	
	// PrivacySchemes is defined in context service config.
	private final PrivacySchemes privacyScheme;
	
	
	private ClientProfilerStatClass clientProf;
	
	/**
	 * Use this constructor if you want to directly communicate with CS, bypassing GNS.
	 * @param csHostName
	 * @param csPortNum
	 * @throws IOException
	 * @throws NoSuchAlgorithmException 
	 */
	public ContextServiceClient( String csHostName, int csPortNum, 
			boolean useGNS, PrivacySchemes privacyScheme )
			throws IOException, NoSuchAlgorithmException
	{
		super( csHostName, csPortNum );
		this.privacyScheme = privacyScheme;
		
		if(useGNS)
		{
			this.initializeGNSClient();
		}
		else
		{
			gnsClient = null;
		}
		
		privacyCallBack = new PrivacyCallBack();
		blockingCallBack = new BlockingCallBack();
		execService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		initializeClient();
		
		if(PROFILER_ENABLE)
		{
			clientProf = new ClientProfilerStatClass();
			new Thread(clientProf).start();
		}
	}
	
	public void sendUpdateWithCallBack
		( String GUID, GuidEntry myGuidEntry, JSONObject gnsAttrValuePairs, 
		long versionNum, UpdateReplyInterface updReplyObj, CallBackInterface callback )
	{
		//Note: gnsAttrValuePairs, key is attrName, value is attrValue
		ContextServiceLogger.getLogger().fine("ContextClient sendUpdate enter "+GUID
				+" json "+gnsAttrValuePairs);
		
		try
		{
			if(gnsClient != null)
			{
				sendUpdateToGNS(myGuidEntry, gnsAttrValuePairs);
			}
			
			JSONObject csAttrValuePairs 
						= filterCSAttributes(gnsAttrValuePairs);
			
			// no context service attribute matching.
			if( csAttrValuePairs.length() <= 0 )
			{
				System.out.println("Number of attributes less than zero");
				return;
			}
			sendUpdateToCS(GUID, 
					csAttrValuePairs, null, versionNum, updReplyObj, callback, 
					PrivacySchemes.NO_PRIVACY.ordinal()
					, null);
		}
		catch ( Exception | Error e )
		{
			e.printStackTrace();
		}
		// no waiting in update	
	}
	
	public void sendSearchQueryWithCallBack(String searchQuery, 
			long expiryTime, SearchReplyInterface searchRep, CallBackInterface callback)
	{
		sendSearchQueryToCS(searchQuery, expiryTime, 
				searchRep, callback, PrivacySchemes.NO_PRIVACY.ordinal());
	}
	
	public JSONObject sendGetRequest(String GUID)
	{
		long currId;
		synchronized(this.getIdLock)
		{
			currId = this.getReqId++;
		}
		
		GetMessage getmesgU 
			= new GetMessage(this.nodeid, currId, GUID, sourceIP, sourcePort);
		
		GetStorage getQ = new GetStorage();
		getQ.requestID = currId;
		getQ.getMessage = getmesgU;
		getQ.getReplyMessage = null;
		
		this.pendingGet.put(currId, getQ);
		InetSocketAddress sockAddr = this.csNodeAddresses.get(rand.nextInt(csNodeAddresses.size()));
		//ContextServiceLogger.getLogger().fine("Sending query to "+sockAddr);
		
		try 
		{
			niot.sendToAddress(sockAddr, getmesgU.toJSONObject());
		} catch (IOException e) 
		{
			e.printStackTrace();
		} catch (JSONException e) 
		{
			e.printStackTrace();
		}
		
		synchronized( getQ )
		{
			while( getQ.getReplyMessage == null )
			{
				try
				{
					getQ.wait();
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
		}
		JSONObject result = pendingGet.get(currId).getReplyMessage.getGUIDObject();
		pendingGet.remove(currId);
		return result;
	}
	
	/**
	 * Blocking call to return the current triggers.
	 * This call is also thread safe, only one thread will be notified though.
	 */
	public void getQueryUpdateTriggers(JSONArray triggerArray)
	{
		if(triggerArray == null)
		{
			assert(false);
			return;
		}
		
		synchronized( refreshTriggerClientWaitLock )
		{
			while( refreshTriggerQueue.size() == 0 )
			{
				try
				{
					refreshTriggerClientWaitLock.wait();
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
			
			while( !refreshTriggerQueue.isEmpty() )
			{
				triggerArray.put(refreshTriggerQueue.poll());
			}
		}
	}
	
	public void sendUpdateSecureWithCallback( String GUID, GuidEntry myGUIDInfo, 
			JSONObject attrValuePairs, long versionNum, 
			HashMap<String, List<ACLEntry>> aclmap, List<AnonymizedIDEntry> anonymizedIDList,
			UpdateReplyInterface updReplyObj, CallBackInterface callback )
	{
		try
		{
			if( gnsClient != null )
			{
				GNSTransformedMessage gnsTransformMesg = 
						this.gnsPrivacyTransform.transformUpdateForGNSPrivacy
						(attrValuePairs, aclmap);
				
				sendUpdateToGNS( myGUIDInfo, 
						gnsTransformMesg.getEncryptedAttrValuePair() );
			}
			
			JSONObject csAttrValuePairs = filterCSAttributes(attrValuePairs);
			// no context service attribute matching.
			if( csAttrValuePairs.length() <= 0 )
			{
				return;
			}
			
			List<CSUpdateTransformedMessage> transformedMesgList 
				= this.asymmetricCSPrivacyTransform.transformUpdateForCSPrivacy
				(myGUIDInfo.getGuid(), attrValuePairs, aclmap, anonymizedIDList);
			
			assert(transformedMesgList.size() > 0);
			
			
			synchronized(printLocks)
			{
				// only measuring for updates, not inserts
				if(attrValuePairs.length() == 1)
				{
					this.sumNumAnonymizedIdsUpdated 
							= this.sumNumAnonymizedIdsUpdated + transformedMesgList.size();
					this.totalPrivacyUpdateReqs++;
				}
			}
			
			
			UpdateReplyInterface privacyUpdRep 
					= new PrivacyUpdateReply( updReplyObj, callback, 
							transformedMesgList.size() );
			
			for( int i=0; i<transformedMesgList.size(); i++ )
			{
				CSUpdateTransformedMessage csTransformedMessage 
									= transformedMesgList.get(i);
				
				sendUpdateToCS( csTransformedMessage.getAnonymizedIDString(), csTransformedMessage.getAttrValJSON(), 
						csTransformedMessage.getAnonymizedIDToGuidMapping(), 
						versionNum, privacyUpdRep, privacyCallBack, privacyScheme.ordinal() ,
						csTransformedMessage.getAnonymizedIDAttrSet() );
			}
			
			
			if( transformedMesgList.size() > 0 )
			{
//				System.out.println
//				("sendUpdateSecure complete GUID "+GUID+" transform time "+(end1-start1)
//					+" total time "+(end2-start1)+
//					" length "+attrValuePairs.length()+" transformedMesgList size "
//					+transformedMesgList.size());
			}
		}
		catch( JSONException jsoEx )
		{
			jsoEx.printStackTrace();
		}
	}
	
	
	public double getAvgAnonymizedIDUpdated()
	{
		return this.sumNumAnonymizedIdsUpdated/this.totalPrivacyUpdateReqs;
	}
	
	public void sendSearchQuerySecureWithCallBack
		( String searchQuery, 
			long expiryTime, GuidEntry myGUIDInfo, 
			SearchReplyInterface searchRep, CallBackInterface callback )
	{
		sendSearchQueryToCS(searchQuery, expiryTime, searchRep, callback, 
				privacyScheme.ordinal());
	}
	
	
	public JSONObject sendGetRequestSecure(String GUID, GuidEntry myGUIDInfo) throws Exception
	{
		// fetch it form GNS
		if(gnsClient != null)
		{
			GNSCommand commandRes = gnsClient.execute(GNSCommand.read(GUID, myGUIDInfo));
			JSONObject encryptedJSON = commandRes.getResultJSONObject();
			GNSTransformedMessage gnsTransformedMesg 
							= new GNSTransformedMessage(encryptedJSON);
			
			return this.gnsPrivacyTransform.unTransformGetReply(gnsTransformedMesg, myGUIDInfo);
		}
		return null;
	}
	
	/**
	 * assumption is that ACL always fits in memory.
	 * If useSymmetricKeys is true then symmetric keys are used.
	 * If useSymmetricKeys is false then asymmetric keys are used.
	 * 
	 * @throws JSONException 
	 */
	public List<AnonymizedIDEntry> computeAnonymizedIDs( GuidEntry myGuidEntry,
			HashMap<String, List<ACLEntry>> aclMap, boolean useSymmetricKeys ) 
					throws JSONException
	{
		List<AnonymizedIDEntry> anonymizedIDList = null;
		
		if(useSymmetricKeys)
		{
			anonymizedIDList 
					= this.symmetricAnonymizedIDCreation.computeAnonymizedIDs
							(myGuidEntry, aclMap);
		}
		else
		{
			anonymizedIDList = this.asymmetricAnonymizedIDCreation.computeAnonymizedIDs
											(myGuidEntry, aclMap);
		}
		
		if(gnsClient != null)
		{
			if( useSymmetricKeys )
			{
				shareSymmetricKeyWithACLMembers(myGuidEntry, anonymizedIDList);
			}
		}
		return anonymizedIDList;
	}
	
	
	@Override
	public void sendUpdate(String GUID, GuidEntry myGuidEntry, 
			JSONObject attrValuePairs, long versionNum) 
	{
		//Note: gnsAttrValuePairs, key is attrName, value is attrValue
		ContextServiceLogger.getLogger().fine( "ContextClient sendUpdate enter "+GUID
				+" json "+attrValuePairs );
		
		try
		{
			if( gnsClient != null )
			{
				sendUpdateToGNS(myGuidEntry, attrValuePairs);
			}
			
			JSONObject csAttrValuePairs 
						= filterCSAttributes(attrValuePairs);
			
			// no context service attribute matching.
			if( csAttrValuePairs.length() <= 0 )
			{
				return;
			}
			
			long currReqId;
			
			synchronized( this.blockingReqIDLock )
			{
				currReqId = this.blockingReqID++;
			}
			
			BlockingUpdateReply blockingUpd = new BlockingUpdateReply(currReqId);
			sendUpdateToCS(GUID, 
					csAttrValuePairs, null, versionNum, blockingUpd, blockingCallBack, 
					PrivacySchemes.NO_PRIVACY.ordinal(), null );
			
			blockingUpd.waitForCompletion();
		}
		catch ( Exception | Error e )
		{
			e.printStackTrace();
		}
	}

	@Override
	public int sendSearchQuery(String searchQuery, JSONArray replyArray, long expiryTime) 
	{	
		if( replyArray == null )
		{
			ContextServiceLogger.getLogger().warning("null passsed "
					+ "as replyArray in sendSearchQuery");
			return -1;
		}
		
		long currBlockingId;
		synchronized( this.blockingReqIDLock )
		{
			currBlockingId = this.blockingReqID++;
		}
		
		BlockingSearchReply blockingSearch = new BlockingSearchReply(currBlockingId);	
		
		sendSearchQueryToCS(searchQuery, expiryTime, 
				blockingSearch, this.blockingCallBack, PrivacySchemes.NO_PRIVACY.ordinal() );
		
		blockingSearch.waitForCompletion();
		
		assert(blockingSearch.getSearchReplyArray() != null);
		for( int i=0; i<blockingSearch.getSearchReplyArray().length(); i++ )
		{
			try
			{
				JSONArray jsoArr1 = blockingSearch.getSearchReplyArray().getJSONArray(i);
				for(int j=0; j<jsoArr1.length(); j++)
				{
					JSONObject searchRepJSON = jsoArr1.getJSONObject(j);
					SearchReplyGUIDRepresentationJSON searchRepObj 
							= SearchReplyGUIDRepresentationJSON.fromJSONObject(searchRepJSON);
					replyArray.put(searchRepObj.getID());
				}
			} catch ( JSONException e )
			{
				e.printStackTrace();
			}
		}
		return blockingSearch.getReplySize();		
	}

	@Override
	public void sendUpdateSecure(String GUID, GuidEntry myGUIDInfo, 
			JSONObject attrValuePairs, long versionNum,
			HashMap<String, List<ACLEntry>> aclmap, List<AnonymizedIDEntry> anonymizedIDList) 
	{
		try
		{
			if(gnsClient != null)
			{
				GNSTransformedMessage gnsTransformMesg = 
						this.gnsPrivacyTransform.transformUpdateForGNSPrivacy
						(attrValuePairs, aclmap);
				
				sendUpdateToGNS( myGUIDInfo, 
						gnsTransformMesg.getEncryptedAttrValuePair() );
			}
			
			JSONObject csAttrValuePairs = filterCSAttributes(attrValuePairs);
			// no context service attribute matching.
			if( csAttrValuePairs.length() <= 0 )
			{
				return;
			}
			
			List<CSUpdateTransformedMessage> transformedMesgList 
				= this.asymmetricCSPrivacyTransform.transformUpdateForCSPrivacy
				(myGUIDInfo.getGuid(), attrValuePairs, aclmap, anonymizedIDList);
			
//			System.out.println(" Size of anonymized ID list for an update "
//												+anonymizedIDList.size()+" "+attrValuePairs);
			
			assert(transformedMesgList.size() > 0);
			
			
			long currblockReqId;
			
			synchronized( this.blockingReqIDLock )
			{
				currblockReqId = this.blockingReqID++;
			}
			
			BlockingUpdateReply blockingUpd = new BlockingUpdateReply(currblockReqId);
			
			
			UpdateReplyInterface privacyUpdRep = new PrivacyUpdateReply( 
					blockingUpd, this.blockingCallBack, transformedMesgList.size());
			
			
			for( int i=0; i<transformedMesgList.size(); i++ )
			{
				CSUpdateTransformedMessage csTransformedMessage 
									= transformedMesgList.get(i);
				
				sendUpdateToCS( csTransformedMessage.getAnonymizedIDString(), 
						csTransformedMessage.getAttrValJSON(), 
						csTransformedMessage.getAnonymizedIDToGuidMapping(), 
						versionNum, privacyUpdRep, privacyCallBack,
						privacyScheme.ordinal(), csTransformedMessage.getAnonymizedIDAttrSet());
			}
			
			if( transformedMesgList.size() > 0 )
			{
//				System.out.println
//				("sendUpdateSecure complete GUID "+GUID+" transform time "+(end1-start1)
//					+" total time "+(end2-start1)+
//					" length "+attrValuePairs.length()+" transformedMesgList size "
//					+transformedMesgList.size() );
			}
			
			blockingUpd.waitForCompletion();
		}
		catch( JSONException jsoEx )
		{
			jsoEx.printStackTrace();
		}
	}
	
	
	@Override
	public int sendSearchQuerySecure(String searchQuery, JSONArray replyArray, 
				long expiryTime, GuidEntry myGUIDInfo)
	{
		if( replyArray == null )
		{
			ContextServiceLogger.getLogger().warning("null passsed "
					+ "as replyArray in sendSearchQuery");
			return -1;
		}
		
		long currBlockingId;
		synchronized( this.blockingReqIDLock )
		{
			currBlockingId = this.blockingReqID++;
		}
		
		BlockingSearchReply blockingSearch = new BlockingSearchReply(currBlockingId);
		
		sendSearchQueryToCS(searchQuery, expiryTime, blockingSearch, blockingCallBack, 
				privacyScheme.ordinal());
		
		blockingSearch.waitForCompletion();
		
		if( this.privacyScheme == PrivacySchemes.PRIVACY )
		{
			List<CSSearchReplyTransformedMessage> searchRepTransformList =  
					computeListForUnTransformFromSearchReply( blockingSearch.getSearchReplyArray() );
			
			if( ContextServiceConfig.DECRYPTIONS_ON_SEARCH_REPLY_ENABLED )
			{
				this.asymmetricCSPrivacyTransform.unTransformSearchReply( myGUIDInfo,
					searchRepTransformList, replyArray );
				
				return replyArray.length();
			}
			else
			{
				return blockingSearch.getReplySize();
			}
		}
//		else if( this.privacyScheme == PrivacySchemes.SUBSPACE_PRIVACY )
//		{
//			processSearchReplyInSubspacePrivacyFromCNS(blockingSearch.getSearchReplyArray(), 
//					replyArray, myGUIDInfo, null);
//			
//			if( ContextServiceConfig.DECRYPTIONS_ON_SEARCH_REPLY_ENABLED )
//			{
//				return replyArray.length();
//			}
//			else
//			{
//				return blockingSearch.getReplySize();
//			}
//		}
		else
		{
			assert(false);
			return -1;
		}
	}
	
	// FIXME: more cleanup needed. a lot of code duplication
	public int sendSearchQuerySecure(String searchQuery, JSONArray replyArray, 
				long expiryTime, HashMap<String, byte[]> anonymizedIDToSecretKeyMap)
	{
		if( replyArray == null )
		{
			ContextServiceLogger.getLogger().warning("null passsed "
					+ "as replyArray in sendSearchQuery");
			return -1;
		}
		
		long currBlockingId;
		synchronized( this.blockingReqIDLock )
		{
			currBlockingId = this.blockingReqID++;
		}
		
		BlockingSearchReply blockingSearch = new BlockingSearchReply(currBlockingId);
		
		sendSearchQueryToCS(searchQuery, expiryTime, blockingSearch, blockingCallBack, 
				privacyScheme.ordinal());
		
		blockingSearch.waitForCompletion();
		
		if( this.privacyScheme == PrivacySchemes.PRIVACY )
		{
			List<CSSearchReplyTransformedMessage> searchRepTransformList 
							= new LinkedList<CSSearchReplyTransformedMessage>();
			
			for(int i=0; i<blockingSearch.getSearchReplyArray().length(); i++)
			{
				try
				{
					JSONArray jsoArr1 = blockingSearch.getSearchReplyArray().getJSONArray(i);
					for( int j=0; j<jsoArr1.length(); j++ )
					{
						JSONObject searchRepJSON = jsoArr1.getJSONObject(j);
	
						if( ContextServiceConfig.DECRYPTIONS_ON_SEARCH_REPLY_ENABLED )
						{
							SearchReplyGUIDRepresentationJSON searchRepObj 
								= SearchReplyGUIDRepresentationJSON.fromJSONObject(searchRepJSON);
							CSSearchReplyTransformedMessage csSearchRepTransform 
								= new CSSearchReplyTransformedMessage(searchRepObj);
							
							searchRepTransformList.add(csSearchRepTransform);
						}
						else
						{
							// just adding the whole JSON here for the user to decrypt later on.
							replyArray.put(searchRepJSON);
						}
					}
				} 
				catch ( JSONException e )
				{
					e.printStackTrace();
				}
			}
			
			
			if( ContextServiceConfig.DECRYPTIONS_ON_SEARCH_REPLY_ENABLED )
			{
				this.symmetricCSPrivacyTransform.unTransformSearchReply
					( anonymizedIDToSecretKeyMap, searchRepTransformList, replyArray );
	//			long end2 = System.currentTimeMillis();
				
	//			System.out.println("SendSearchQuerySecure search reply from CS time "+ 
	//						(end1-start)+" reply decryption time "+(end2-end1)+" fromCS reply size "
	//						+ searchRepTransformList.size()+" final reply size "+replyArray.length() );
				
				return replyArray.length();
			}
			else
			{
	//			long end2 = System.currentTimeMillis();		
	//			System.out.println("SendSearchQuerySecure search reply from CS time "+ 
	//						(end1-start)+" reply decryption time "+(end2-end1)+" fromCS reply size "
	//						+ searchRepTransformList.size()+" final reply size "+replyArray.length() );
				
				return blockingSearch.getReplySize();
			}
		}
//		else if( this.privacyScheme == PrivacySchemes.SUBSPACE_PRIVACY  )
//		{
//			processSearchReplyInSubspacePrivacyFromCNS(blockingSearch.getSearchReplyArray(), 
//					replyArray, null, anonymizedIDToSecretKeyMap);
//			
//			if( ContextServiceConfig.DECRYPTIONS_ON_SEARCH_REPLY_ENABLED )
//			{
//				return replyArray.length();
//			}
//			else
//			{
//				return blockingSearch.getReplySize();
//			}
//		}
		else
		{
			assert(false);
			return -1;
		}
	}
	
	
	private void processSearchReplyInSubspacePrivacyFromCNS(JSONArray replyFromCNS, 
			JSONArray userReplyArray, GuidEntry guidEntry, 
			HashMap<String, byte[]> anonymizedIDToSecretKeyMap)
	{
		// in subspace privacy scheme. JSONArray is a JSONArray of replies for each subspace.
		// So we decrypt each JSONArray corresponding to a subspace one by one.
		
		HashMap<String, Boolean> resultMap = null;
		
		for( int i=0; i < replyFromCNS.length(); i++ )
		{
			try 
			{
				JSONArray subspaceReplyArray 
						= replyFromCNS.getJSONArray(i);
				
				List<CSSearchReplyTransformedMessage> searchRepTransformList =  
						computeListForUnTransformFromSearchReply( subspaceReplyArray );
				
				
				if( ContextServiceConfig.DECRYPTIONS_ON_SEARCH_REPLY_ENABLED )
				{
					HashMap<String, Boolean> subspaceReplyMap = new HashMap<String, Boolean>();
					
					if(guidEntry != null)
					{
						this.asymmetricCSPrivacyTransform.unTransformSearchReply( guidEntry,
								searchRepTransformList, subspaceReplyMap );
					}
					else if(anonymizedIDToSecretKeyMap != null)
					{
						this.symmetricCSPrivacyTransform.unTransformSearchReply( 
								anonymizedIDToSecretKeyMap,
								searchRepTransformList, subspaceReplyMap );
					}
						
					
					assert(subspaceReplyMap != null);
					
					
					// simple impl for conjunction. 
					// as result size increases we might need to switch to bloom filter or 
					// or some bit array approach.
					if(i == 0)
					{
						resultMap = subspaceReplyMap;
					}
					else
					{
						HashMap<String, Boolean> conjuncMap = new HashMap<String, Boolean>();
						
						Iterator<String> resultSoFarIter = resultMap.keySet().iterator();
						
						while( resultSoFarIter.hasNext() )
						{
							String guidToCheck = resultSoFarIter.next();
							
							// GUID from result so far should also be in the reply from  current subspace.
							if( subspaceReplyMap.containsKey(guidToCheck) )
							{
								conjuncMap.put(guidToCheck, true);
							}
						}
						resultMap = conjuncMap;
					}
					
					if(resultMap.size() == 0)
					{
						// conjunction so far has led to empty result.
						// no need to continue in loop result will be empty.
						break;
					}
				}
			}
			catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
		
		assert(resultMap != null);
		
		if( ContextServiceConfig.DECRYPTIONS_ON_SEARCH_REPLY_ENABLED )
		{
			Iterator<String> guidIter = resultMap.keySet().iterator();
			while( guidIter.hasNext() )
			{
				userReplyArray.put(guidIter.next());
			}
		}
	}
	
	@Override
	public boolean handleMessage(JSONObject jsonObject, NIOHeader nioHeader)
	{
		(new HandleMessageThread(jsonObject)).run();
		return true;
	}
	
	/**
	 * Returns GNSClient if Context service client was created
	 * by setting useGNS to true.
	 * @return
	 */
	public GNSClient getGNSClient()
	{
		return this.gnsClient;
	}
	
	
	/**
	 * Reads the SYMMETRIC_KEY_EXCHANGE_FIELD_NAME from GNS 
	 * from the provided GuidEntry.
	 * Decrypts the field using the private key and 
	 * returns a map of anonymized ID and the corresponding symmetric key to use.
	 * Symmetric key is in byte[] format.
	 * @param guidEntry
	 */
	public HashMap<String, byte[]> getAnonymizedIDToSymmetricKeyMapFromGNS(GuidEntry guidEntry)
	{
		HashMap<String, byte[]> anonymizedIDToSecretKeyMap 
										= new HashMap<String, byte[]>();
										
		try
		{
			GNSCommand commandRes = gnsClient.execute
					( GNSCommand.fieldReadArray(guidEntry.getGuid(), 
							SYMMETRIC_KEY_EXCHANGE_FIELD_NAME, guidEntry) );
			
			System.out.println("commandRes "+commandRes.getResult());
			JSONArray resultArray = commandRes.getResultJSONObject().getJSONArray(SYMMETRIC_KEY_EXCHANGE_FIELD_NAME);
			
			for( int i=0; i < resultArray.length(); i++ )
			{
				try
				{
					String encryptedValString = resultArray.getString(i);
					byte[] encryptedByteArray = Utils.hexStringToByteArray(encryptedValString);
					byte[] plainText = Utils.doPrivateKeyDecryption( guidEntry.getPrivateKey().getEncoded(), 
							encryptedByteArray );
					
					assert(plainText != null);
					
					// first ContextServiceConfig.SIZE_OF_ANONYMIZED_ID bytes are anonymized 
					// ID.
					assert(plainText.length > ContextServiceConfig.SIZE_OF_ANONYMIZED_ID);
					
					
					// in plain text, first 20 bytes are the anonymized ID.
					// rest of the byte[] is the symmetric key.
					byte[] anonymizedID = new byte[ContextServiceConfig.SIZE_OF_ANONYMIZED_ID]; 
					System.arraycopy(plainText, 0, anonymizedID, 0, 
										ContextServiceConfig.SIZE_OF_ANONYMIZED_ID);
					
					int symmetricKeyLength = plainText.length 
											- ContextServiceConfig.SIZE_OF_ANONYMIZED_ID;
					
					assert(symmetricKeyLength > 0);
					
					byte[] symmetricKey = new byte[symmetricKeyLength];
					
					System.arraycopy(plainText, ContextServiceConfig.SIZE_OF_ANONYMIZED_ID,
							symmetricKey, 0, symmetricKeyLength);
					
					String anoymizedIDString = Utils.byteArrayToHex(anonymizedID);
					
					if( anonymizedIDToSecretKeyMap.containsKey(anoymizedIDString) )
					{
						// not sure what to do here, if there are multiple
						// entries for an anonymized iD.
						// So failing the assertion.
						assert(false);
					}
					else
					{
						anonymizedIDToSecretKeyMap.put(anoymizedIDString, symmetricKey);
					}
				}
				catch (JSONException | InvalidKeyException | NoSuchAlgorithmException | 
						InvalidKeySpecException | NoSuchPaddingException | 
						IllegalBlockSizeException | BadPaddingException e) 
				{
					e.printStackTrace();
				}
			}
		} catch (ClientException | IOException | JSONException e) 
		{
			e.printStackTrace();
		}
		// map could very well be empty if this GUID is
		// not in any ACL
		return anonymizedIDToSecretKeyMap;
	}
	
	
	private List<CSSearchReplyTransformedMessage> 
				computeListForUnTransformFromSearchReply( JSONArray replyArray )
	{
		List<CSSearchReplyTransformedMessage> searchRepTransformList 
							= new LinkedList<CSSearchReplyTransformedMessage>();
		
		for( int i=0; i<replyArray.length(); i++ )
		{
			try
			{
				JSONArray jsoArr1 = replyArray.getJSONArray(i);
				for( int j=0; j<jsoArr1.length(); j++ )
				{
					JSONObject searchRepJSON = jsoArr1.getJSONObject(j);
				
					if( ContextServiceConfig.DECRYPTIONS_ON_SEARCH_REPLY_ENABLED )
					{
						SearchReplyGUIDRepresentationJSON searchRepObj 
								= SearchReplyGUIDRepresentationJSON.fromJSONObject(searchRepJSON);
						CSSearchReplyTransformedMessage csSearchRepTransform 
								= new CSSearchReplyTransformedMessage(searchRepObj);
						
						searchRepTransformList.add(csSearchRepTransform);
					}
					else
					{
						// just adding the whole JSON here for the user to decrypt later on.
						replyArray.put(searchRepJSON);
					}
				}
			}
			catch ( JSONException e )
			{
				e.printStackTrace();
			}
		}
		
		return searchRepTransformList;
	}
	
	
	private void initializeGNSClient()
	{
		// FIXME: so that we don't have to hard code values here.
		// just setting some gns properties.
		Properties props = System.getProperties();
		props.setProperty("gigapaxosConfig", "conf/gnsClientConf/gnsclient.local.properties");
		props.setProperty("javax.net.ssl.trustStorePassword", "qwerty");
		props.setProperty("javax.net.ssl.trustStore", "conf/gnsClientConf/trustStore.jks");
		props.setProperty("javax.net.ssl.keyStorePassword", "qwerty");
		props.setProperty("javax.net.ssl.keyStore", "conf/gnsClientConf/keyStore.jks");
		
		try 
		{
			gnsClient = new GNSClientCommands();
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * Shares symmetric keys with ACL members of a GUID. 
	 * @param anonymizedIDList
	 */
	private void shareSymmetricKeyWithACLMembers(GuidEntry myGuidEntry, 
			List<AnonymizedIDEntry> anonymizedIDList)
	{
		for( int i=0; i<anonymizedIDList.size(); i++ )
		{
			AnonymizedIDEntry anonymizedIDEntry = anonymizedIDList.get(i);
			
			// key is GUID in string, value is the enc(anonymizedID||encrypted symmetric key) in String 
			HashMap<String, String> secretKeySharingMap = anonymizedIDEntry.getSecretKeySharingMap();
			
			Iterator<String> guidIter = secretKeySharingMap.keySet().iterator();
			
			while( guidIter.hasNext() )
			{
				String guidString = guidIter.next();
				String encryptedSecretKey = secretKeySharingMap.get(guidString);
				
				try 
				{
					guidString = guidString.toUpperCase();
					System.out.println("guidString "+guidString);
					
					gnsClient.execute(GNSCommand.fieldAppend( guidString, SYMMETRIC_KEY_EXCHANGE_FIELD_NAME, 
							encryptedSecretKey, myGuidEntry ));
				} catch (ClientException | IOException e) 
				{
					e.printStackTrace();
				}
			}
			
		}
	}
	
	/**
	 * Sends update to CS in both privacy and non privacy case.
	 * filtering of CS attributes has already happened before this function call.
	 * It assumes all attributes in csAttrValMap are CS attributes.
	 * anonymizedIDAttrSet can be empty or null for non privacy updates.
	 */
	private void sendUpdateToCS( String GUID, 
			JSONObject csAttrValPair, JSONArray anonymizedIDToGuidMappingArray, 
			long versionNum, UpdateReplyInterface updReplyObj, CallBackInterface callback, 
			int privacyScheme, JSONArray anonymizedIDAttrSet )
	{
		try
		{
			long currId;
			
			synchronized( this.updateIdLock )
			{
				currId = this.updateReqId++;
			}
			long requestID = currId;
			
			ValueUpdateFromGNS valUpdFromGNS = new
					ValueUpdateFromGNS( null, versionNum, GUID, 
							csAttrValPair, requestID, sourceIP, sourcePort, 
							System.currentTimeMillis(), anonymizedIDToGuidMappingArray, privacyScheme,
							anonymizedIDAttrSet );
			
			UpdateStorage updateQ = new UpdateStorage();
			updateQ.requestID = currId;
			updateQ.valUpdFromGNS = valUpdFromGNS;
			updateQ.valUpdFromGNSReply = null;
			updateQ.updReplyObj = updReplyObj;
			updateQ.callback = callback;
			//updateQ.blocking = blocking;
			
			this.pendingUpdate.put(currId, updateQ);
			
			InetSocketAddress sockAddr 
						= this.csNodeAddresses.get(rand.nextInt(csNodeAddresses.size()));
			
			ContextServiceLogger.getLogger().fine("ContextClient sending update requestID "+currId+" to "+sockAddr+" json "+
					valUpdFromGNS);
			if(PROFILER_ENABLE)
			{
				this.clientProf.incrementNumUpdateSent();
			}
			niot.sendToAddress(sockAddr, valUpdFromGNS.toJSONObject());
		}
		catch ( Exception | Error e )
		{
			e.printStackTrace();
		}
	}
	
	private void sendUpdateToGNS(GuidEntry writingGuid, 
			JSONObject attrValuePair)
	{
		if(gnsClient != null)
		{
			try 
			{
				gnsClient.execute(GNSCommand.update(writingGuid, attrValuePair));
			} catch (IOException e) 
			{
				e.printStackTrace();
			} catch (ClientException e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	
	private void sendSearchQueryToCS(String searchQuery, 
			long expiryTime, SearchReplyInterface searchRep, 
			CallBackInterface callback, int privacySchemeOrdinal )
	{
		long currId;
		synchronized( this.searchIdLock )
		{
			currId = this.searchReqId++;
		}
		
		QueryMsgFromUser qmesgU 
			= new QueryMsgFromUser(this.nodeid, searchQuery, 
					currId, expiryTime, sourceIP, sourcePort, privacySchemeOrdinal);
		
		SearchQueryStorage searchQ = new SearchQueryStorage();
		searchQ.requestID = currId;
		searchQ.queryMsgFromUser = qmesgU;
		searchQ.queryMsgFromUserReply = null; 
		searchQ.searchRep = searchRep;
		searchQ.callback = callback;
		
		this.pendingSearches.put(currId, searchQ);
		InetSocketAddress sockAddr 
				= this.csNodeAddresses.get(rand.nextInt(csNodeAddresses.size()));
		
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
	
	
	/**
	 * filters attributes, returns only attributes value pairs 
	 * that are supported by CS
	 * @param gnsAttrValuePairs
	 * @return
	 * @throws JSONException
	 */
	private JSONObject filterCSAttributes(JSONObject gnsAttrValuePairs) throws JSONException
	{
		//filetering out CS attrbutes.
		JSONObject csAttrValuePairs = new JSONObject();
		// JSON iterator warning suppressed.
		@SuppressWarnings("rawtypes")
		Iterator gnsIter = gnsAttrValuePairs.keys();
		
		while( gnsIter.hasNext() )
		{
			String gnsAttrName = (String) gnsIter.next();
			if( attributeHashMap.containsKey(gnsAttrName) )
			{
				csAttrValuePairs.put(gnsAttrName, gnsAttrValuePairs.get(gnsAttrName));
			}
		}
		return csAttrValuePairs;
	}
	
	private void initializeClient() throws NoSuchAlgorithmException
	{	
		refreshTriggerQueue = new LinkedList<JSONObject>();
		// FIXME: add a timeout mechanism here.
		sendConfigRequest();
		
		switch( privacyScheme )
		{
			case NO_PRIVACY:
			{
				break;
			}
			
			case PRIVACY:
			{
				this.asymmetricAnonymizedIDCreation 
								= new HyperspaceBasedASymmetricKeyAnonymizedIDCreator();
				
				
				this.asymmetricCSPrivacyTransform 
								= new HyperspaceBasedASymmetricKeyCSTransform(execService);
				
				
				this.symmetricAnonymizedIDCreation 
							= new HyperspaceBasedSymmetricKeyAnonymizedIDCreator();
				
				
				this.symmetricCSPrivacyTransform 
							= new HyperspaceBasedSymmetricKeyCSTransform(execService);
				
				break;
			}
			
//			case SUBSPACE_PRIVACY:
//			{	
//				this.asymmetricAnonymizedIDCreation 
//						= new SubspaceBasedASymmetricKeyAnonymizedIDCreator(this.subspaceAttrMap);
//
//
//				this.asymmetricCSPrivacyTransform 
//						= new SubspaceBasedASymmetricKeyCSTransform(execService);
//
//
//				this.symmetricAnonymizedIDCreation 
//						= new SubspaceBasedSymmetricKeyAnonymizedIDCreator(subspaceAttrMap);
//
//
//				this.symmetricCSPrivacyTransform 
//						= new SubspaceBasedSymmetricKeyCSTransform(execService);
//				
//				break;
//			}
		}
		
		
		// for gnsTransform
		gnsPrivacyTransform = new EncryptionBasedGNSPrivacyTransform();
	}
	
	private void sendConfigRequest()
	{	
		do
		{
			ClientConfigRequest clientConfigReq 
				= new ClientConfigRequest(nodeid, sourceIP, sourcePort);
			
			InetSocketAddress sockAddr = new InetSocketAddress(configHost, configPort);
			
			try 
			{
				niot.sendToAddress(sockAddr, clientConfigReq.toJSONObject());
			} catch (IOException e) 
			{
				e.printStackTrace();
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
			
			synchronized( configLock )
			{
				while( csNodeAddresses.size() == 0 )
				{
					try 
					{
						configLock.wait(5000);
						break;
					} catch (InterruptedException e) 
					{
						e.printStackTrace();
						break;
					}
				}
			}
			if(csNodeAddresses.size() == 0)
			System.out.println("Contect service client failed to get config. Trying again");
		} while(csNodeAddresses.size() == 0);
	}
	
	public void printTriggerStats()
	{
		double avgAddTrigger     = this.sumAddedGroupGUIDsOnUpdate/this.numTriggers;
		double avgRemovedTrigger = this.sumRemovedGroupGUIDsOnUpdate/this.numTriggers;
		
		System.out.println("avgAddTrigger "+avgAddTrigger
						+" avgRemovedTrigger "+avgRemovedTrigger);
	}
	
	
	private class HandleMessageThread implements Runnable
	{
		private final JSONObject mesgJSON;
		public HandleMessageThread(JSONObject mesgJSON)
		{
			this.mesgJSON = mesgJSON;
		}
		
		@Override
		public void run()
		{
			try
			{
				if( mesgJSON.getInt(ContextServicePacket.PACKET_TYPE) 
						== ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY.getInt() )
				{
					handleQueryReply(mesgJSON);
				} else if( mesgJSON.getInt(ContextServicePacket.PACKET_TYPE)
						== ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY.getInt() )
				{
					handleUpdateReply(mesgJSON);
				} else if( mesgJSON.getInt(ContextServicePacket.PACKET_TYPE)
						== ContextServicePacket.PacketType.REFRESH_TRIGGER.getInt() )
				{
					handleRefreshTrigger(mesgJSON);
				} else if( mesgJSON.getInt(ContextServicePacket.PACKET_TYPE)
						== ContextServicePacket.PacketType.GET_REPLY_MESSAGE.getInt() )
				{
					handleGetReply(mesgJSON);
				} else if(mesgJSON.getInt(ContextServicePacket.PACKET_TYPE)
						== ContextServicePacket.PacketType.CONFIG_REPLY.getInt() )
				{
					handleConfigReply(mesgJSON);
				}
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		private void handleUpdateReply(JSONObject jso)
		{
			ValueUpdateFromGNSReply vur = null;
			try
			{
				vur = new ValueUpdateFromGNSReply(jso);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			long currReqID = vur.getUserReqNum();
			ContextServiceLogger.getLogger().fine("Update reply recvd "+currReqID);
			UpdateStorage replyUpdObj = pendingUpdate.get(currReqID);
			
			
			if( replyUpdObj != null )
			{
				if(PROFILER_ENABLE)
				{
					clientProf.incrementUpdateRepRecvd();
				}
				
				replyUpdObj.valUpdFromGNSReply = vur;
				replyUpdObj.callback.updateCompletion(replyUpdObj.updReplyObj);
				
				pendingUpdate.remove(currReqID);
			}
			else
			{
				ContextServiceLogger.getLogger().fine("Update reply recvd "
												+currReqID+" update Obj null");
				assert(false);
			}
		}
		
		private void handleGetReply(JSONObject jso)
		{
			try
			{
				GetReplyMessage getReply;
				getReply = new GetReplyMessage(jso);
				
				long reqID = getReply.getReqID();
				GetStorage replyGetObj = pendingGet.get(reqID);
				replyGetObj.getReplyMessage = getReply;
				
				synchronized(replyGetObj)
				{
					replyGetObj.notify();
				}
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		private void handleConfigReply(JSONObject jso)
		{
			try
			{
				synchronized( configLock )
				{
					ClientConfigReply configReply
											= new ClientConfigReply(jso);
					if(csNodeAddresses.size() > 0)
					{
						configLock.notify();
						return;
					}
					
					JSONArray nodeIPArray 		= configReply.getNodeConfigArray();
					JSONArray attrInfoArray 	= configReply.getAttributeArray();
					JSONArray subspaceInfoArray = configReply.getSubspaceInfoArray();
					
					for( int i=0;i<nodeIPArray.length();i++ )
					{
						String ipPort = nodeIPArray.getString(i);
						String[] parsed = ipPort.split(":");
						csNodeAddresses.add(new InetSocketAddress(parsed[0], 
								Integer.parseInt(parsed[1])));
					}
					
					for(int i=0;i<attrInfoArray.length();i++)
					{
						String attrName = attrInfoArray.getString(i);
						attributeHashMap.put(attrName, true);	
					}
					
					for( int i=0; i<subspaceInfoArray.length(); i++ )
					{
						JSONArray subspaceAttrJSON = subspaceInfoArray.getJSONArray(i);
						subspaceAttrMap.put(i, subspaceAttrJSON);
					}
					configLock.notify();
				}
			} catch ( JSONException e )
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
				SearchQueryStorage replySearchObj 
										= pendingSearches.get(reqID);
				replySearchObj.queryMsgFromUserReply = qmur;
				
				JSONArray result = qmur.getResultGUIDs();
				int resultSize = qmur.getReplySize();
				
				// FIXME: in privacy case the client must do decryption and conjunction here.
				// But if we are measuring server capacity and have limited clients to send 
				// requests then we can skip that. 
				replySearchObj.searchRep.setSearchReplyArray(result);
				replySearchObj.searchRep.setReplySize(resultSize);
				
				replySearchObj.callback.searchCompletion(replySearchObj.searchRep);
				
				pendingSearches.remove(reqID);
			}
			catch ( JSONException e )
			{
				e.printStackTrace();
			}
		}
		
		private void handleRefreshTrigger(JSONObject jso)
		{
			try
			{
				RefreshTrigger qmur 
							= new RefreshTrigger(jso);
				
				synchronized( refreshTriggerClientWaitLock )
				{
					if( ContextServiceClient.EXPERIMENT_MODE )
					{
//						private double sumAddedGroupGUIDsOnUpdate						= 0.0;
//						private double sumRemovedGroupGUIDsOnUpdate						= 0.0;
//						private long numTriggers										= 0;
						
						sumAddedGroupGUIDsOnUpdate = sumAddedGroupGUIDsOnUpdate
											+ qmur.getNumAdded();
						
						sumRemovedGroupGUIDsOnUpdate = sumRemovedGroupGUIDsOnUpdate 
											+ qmur.getNumRemoved();
						numTriggers++;
						
						long currTime = System.currentTimeMillis();
						if( (currTime -lastPrintTime) >= 10000 )
						{
							double avgAddTrigger     = sumAddedGroupGUIDsOnUpdate/numTriggers;
							double avgRemovedTrigger = sumRemovedGroupGUIDsOnUpdate/numTriggers;
							
							System.out.println("Time "+(currTime-startTime)+" avgAddTrigger "+avgAddTrigger
											+" avgRemovedTrigger "+avgRemovedTrigger);
							
							sumAddedGroupGUIDsOnUpdate = 0;
							sumRemovedGroupGUIDsOnUpdate = 0;
							numTriggers = 1;
							lastPrintTime = currTime;
						}
					}
					else
					{
						refreshTriggerQueue.add(qmur.toJSONObject());
						refreshTriggerClientWaitLock.notify();
					}
				}
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	
	// testing secure client code
	public static void main(String[] args) 
			throws Exception
	{
		// testing based on the example in the draft.
		// more testing of each method in secure interface.
		// test with the example in the draft.
		//GUIDEntryStoringClass myGUIDInfo, JSONArray ACLArray
		//String guid = GuidUtils.createGuidFromPublicKey(keyPair.getPublic().getEncoded());
//		Properties props = System.getProperties();
//		props.setProperty("gigapaxosConfig", "conf/gnsClientConf/gigapaxos.client.local.properties");
//		props.setProperty("javax.net.ssl.trustStorePassword", "qwerty");
//		props.setProperty("javax.net.ssl.trustStore", "conf/gnsClientConf/trustStore/node100.jks");
//		props.setProperty("javax.net.ssl.keyStorePassword", "qwerty");
//		props.setProperty("javax.net.ssl.keyStore", "conf/gnsClientConf/keyStore/node100.jks");
//		
//		InetSocketAddress address 
//			= new InetSocketAddress("127.0.0.1", GNSClientConfig.LNS_PORT);
//		UniversalTcpClientExtended gnsClient = new GNSClient(null, address, true);
//	
//		GuidEntry masterGuid = GuidUtils.lookupOrCreateAccountGuid(gnsClient,
//            "ayadav@cs.umass.edu", "password", true);
	
		KeyPairGenerator kpg;
		kpg = KeyPairGenerator.getInstance("RSA");
		KeyPair kp0 = kpg.genKeyPair();
		PublicKey publicKey0 = kp0.getPublic();
		PrivateKey privateKey0 = kp0.getPrivate();
		
//		System.out.println("privateKey0 "+((RSAPrivateKey)privateKey0).getModulus().bitLength());
		byte[] publicKeyByteArray0 = publicKey0.getEncoded();
		//byte[] privateKeyByteArray0 = privateKey0.getEncoded();
		
		String guid0 = Utils.convertPublicKeyToGUIDString(publicKeyByteArray0);
		GuidEntry myGUID = new GuidEntry("Guid0", guid0, publicKey0, privateKey0);
		
//		PublicKey publicKey0 = masterGuid.getPublicKey();
//		PrivateKey privateKey0 = masterGuid.getPrivateKey();
//		byte[] publicKeyByteArray0 = publicKey0.getEncoded();
//		byte[] privateKeyByteArray0 = privateKey0.getEncoded();
//		String guid0 = GuidUtils.createGuidFromPublicKey(publicKeyByteArray0);
//		GuidEntry myGUID = masterGuid;	
		
		Vector<GuidEntry> guidsVector = new Vector<GuidEntry>();

		
		guidsVector.add(myGUID);
		
		// draft example has 7 guids
		for(int i=1; i <= 7; i++)
		{
			KeyPair kp = kpg.genKeyPair();
			PublicKey publicKey = kp.getPublic();
			PrivateKey privateKey = kp.getPrivate();
			byte[] publicKeyByteArray = publicKey.getEncoded();
			//byte[] privateKeyByteArray = privateKey.getEncoded();
			
			String guid = Utils.convertPublicKeyToGUIDString(publicKeyByteArray);
			
			GuidEntry currGUID = new GuidEntry("Guid"+i, guid, 
					publicKey, privateKey);
			
			guidsVector.add(currGUID);
		}
		
		HashMap<String, List<ACLEntry>> aclMap = new HashMap<String, List<ACLEntry>>();
		
		List<ACLEntry> acl0 = new LinkedList<ACLEntry>();
		
		acl0.add(new ACLEntry(guidsVector.get(1).getGuid(), guidsVector.get(1).getPublicKey()));
		acl0.add(new ACLEntry(guidsVector.get(2).getGuid(), guidsVector.get(2).getPublicKey()));
		acl0.add(new ACLEntry(guidsVector.get(3).getGuid(), guidsVector.get(3).getPublicKey()));
		aclMap.put("attr1", acl0);
		
		
		List<ACLEntry> acl1 = new LinkedList<ACLEntry>();
		acl1.add(new ACLEntry(guidsVector.get(4).getGuid(), guidsVector.get(4).getPublicKey()));
		acl1.add(new ACLEntry(guidsVector.get(5).getGuid(), guidsVector.get(5).getPublicKey()));
		acl1.add(new ACLEntry(guidsVector.get(3).getGuid(), guidsVector.get(3).getPublicKey()));
		aclMap.put("attr2", acl1);
		
		
		List<ACLEntry> acl2 = new LinkedList<ACLEntry>();
		acl2.add(new ACLEntry(guidsVector.get(1).getGuid(), guidsVector.get(1).getPublicKey()));
		acl2.add(new ACLEntry(guidsVector.get(2).getGuid(), guidsVector.get(2).getPublicKey()));
		aclMap.put("attr3", acl2);
		
		
		List<ACLEntry> acl3 = new LinkedList<ACLEntry>();
		acl3.add(new ACLEntry(guidsVector.get(1).getGuid(), guidsVector.get(1).getPublicKey()));
		acl3.add(new ACLEntry(guidsVector.get(2).getGuid(), guidsVector.get(2).getPublicKey()));
		acl3.add(new ACLEntry(guidsVector.get(3).getGuid(), guidsVector.get(3).getPublicKey()));
		aclMap.put("attr4", acl3);
		
		
		List<ACLEntry> acl4 = new LinkedList<ACLEntry>();
		acl4.add(new ACLEntry(guidsVector.get(6).getGuid(), guidsVector.get(6).getPublicKey()));
		acl4.add(new ACLEntry(guidsVector.get(7).getGuid(), guidsVector.get(7).getPublicKey()));
		aclMap.put("attr5", acl4);
		
		
		List<ACLEntry> acl5 = new LinkedList<ACLEntry>();
		acl5.add(new ACLEntry(guidsVector.get(4).getGuid(), guidsVector.get(4).getPublicKey()));
		acl5.add(new ACLEntry(guidsVector.get(5).getGuid(), guidsVector.get(5).getPublicKey()));
		acl5.add(new ACLEntry(guidsVector.get(1).getGuid(), guidsVector.get(1).getPublicKey()));
		
		aclMap.put("attr6", acl5);
		
		
		ContextServiceClient csClient = new ContextServiceClient
			("127.0.0.1", 8000, false, PrivacySchemes.PRIVACY);
		
		CallBackInterface callback = new NoopCallBack();
		
		
		List<AnonymizedIDEntry> anonymizedIdList = csClient.computeAnonymizedIDs
					(myGUID, aclMap, true);
		
		JSONObject attrValPair = new JSONObject();
		attrValPair.put("attr1", 10+"");
		
		attrValPair.put("attr2", 15+"");
		
		UpdateReplyInterface updateRep = new NoopUpdateReply();
		
		csClient.sendUpdateSecureWithCallback(guid0, 
				myGUID, attrValPair, -1, aclMap, anonymizedIdList, updateRep, callback);
		
		Thread.sleep(2000);
		
		String searchQuery = "attr1 >= 5 AND attr1 <= 15";
		
		GuidEntry queryingGuid = guidsVector.get(3);
		
		SearchReplyInterface searchRep = new NoopSearchReply(2);
		
		csClient.sendSearchQuerySecureWithCallBack
					(searchQuery, 300000, queryingGuid, searchRep, callback);
		
//		System.out.println("Query for attr1 querying GUID "+ queryingGuid.getGuid()+
//				" Real GUID "+guid0+" reply Arr "+replyArray);

		
		searchQuery = "attr1 >= 5 AND attr1 <= 15" + " AND attr2 >= 10 AND attr2 <= 20";
		
		queryingGuid = guidsVector.get(1);
		
		searchRep = new NoopSearchReply(3);
		
		csClient.sendSearchQuerySecureWithCallBack(searchQuery, 300000, queryingGuid,
				searchRep, callback);
	}
}