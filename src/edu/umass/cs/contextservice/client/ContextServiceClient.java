package edu.umass.cs.contextservice.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.anonymizedID.AnonymizedIDCreationInterface;
import edu.umass.cs.contextservice.client.anonymizedID.SubspaceBasedAnonymizedIDCreator;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.client.csprivacytransform.CSPrivacyTransformInterface;
import edu.umass.cs.contextservice.client.csprivacytransform.CSSearchReplyTransformedMessage;
import edu.umass.cs.contextservice.client.csprivacytransform.CSUpdateTransformedMessage;
import edu.umass.cs.contextservice.client.csprivacytransform.ParallelUpdateStateStorage;
import edu.umass.cs.contextservice.client.csprivacytransform.SubspaceBasedCSTransform;
import edu.umass.cs.contextservice.client.gnsprivacytransform.EncryptionBasedGNSPrivacyTransform;
import edu.umass.cs.contextservice.client.gnsprivacytransform.GNSPrivacyTransformInterface;
import edu.umass.cs.contextservice.client.gnsprivacytransform.GNSTransformedMessage;
import edu.umass.cs.contextservice.client.storage.GetStorage;
import edu.umass.cs.contextservice.client.storage.SearchQueryStorage;
import edu.umass.cs.contextservice.client.storage.UpdateStorage;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
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
import edu.umass.cs.contextservice.messages.dataformat.AttrValueRepresentationJSON;
import edu.umass.cs.contextservice.messages.dataformat.ParsingMethods;
import edu.umass.cs.contextservice.messages.dataformat.SearchReplyGUIDRepresentationJSON;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClientExtended;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.GnsClientException;

/**
 * Contextservice client.
 * It is used to send and recv replies from context service.
 * It knows context service node addresses from a file in the conf folder.
 * It is thread safe, means same client can be used by multiple threads without any 
 * synchronization problems.
 * @author adipc
 * @param <NodeIDType>
 */
public class ContextServiceClient<NodeIDType> extends AbstractContextServiceClient<NodeIDType> 
												implements ContextClientInterfaceWithPrivacy, ContextServiceClientInterfaceWithoutPrivacy
{
	public static final int NUM_THREADS = 200;
	
	private Queue<JSONObject> refreshTriggerQueue;
	//private final Object refreshQueueLock 						= new Object();
	
	private final Object refreshTriggerClientWaitLock 				= new Object();
	
	private final UniversalTcpClientExtended gnsClient;
	
	// for anonymized ID
	private AnonymizedIDCreationInterface anonymizedIDCreation;
	
	//gns transform
	private GNSPrivacyTransformInterface gnsPrivacyTransform;
	
	// for cs transform
	private CSPrivacyTransformInterface csPrivacyTransform;
	
	private ExecutorService executorService;
	/**
	 * Use this constructor if you want to directly communicate with CS, bypassing GNS.
	 * @param csHostName
	 * @param csPortNum
	 * @throws IOException
	 * @throws NoSuchAlgorithmException 
	 */
	public ContextServiceClient(String csHostName, int csPortNum) 
			throws IOException, NoSuchAlgorithmException
	{
		super( csHostName, csPortNum );
		gnsClient = null;
		executorService = Executors.newFixedThreadPool(NUM_THREADS);
		initializeClient();
	}
	
	/**
	 * Use this constructor when CS and GNS are used.
	 * @param csHostName
	 * @param csPortNum
	 * @param gnsHostName
	 * @param gnsPort
	 * @throws IOException
	 * @throws NoSuchAlgorithmException 
	 */
	public ContextServiceClient( String csHostName, int csPortNum, 
			String gnsHostName, int gnsPort ) 
			throws IOException, NoSuchAlgorithmException
	{
		super( csHostName, csPortNum );
		
		// just setting some gns properties.
		Properties props = System.getProperties();
		props.setProperty("gigapaxosConfig", "conf/gnsClientConf/gigapaxos.client.local.properties");
		props.setProperty("javax.net.ssl.trustStorePassword", "qwerty");
		props.setProperty("javax.net.ssl.trustStore", "conf/gnsClientConf/trustStore/node100.jks");
		props.setProperty("javax.net.ssl.keyStorePassword", "qwerty");
		props.setProperty("javax.net.ssl.keyStore", "conf/gnsClientConf/keyStore/node100.jks");	
		
		InetSocketAddress gnsAddress 
					= new InetSocketAddress(gnsHostName, gnsPort);
		
		gnsClient = new GNSClient(null, gnsAddress, true);
		
		initializeClient();
	}
	
	public void sendUpdate( String GUID, GuidEntry myGuidEntry, JSONObject gnsAttrValuePairs, 
			long versionNum, boolean blocking )
	{
		//Note: gnsAttrValuePairs, key is attrName, value is attrValue
		ContextServiceLogger.getLogger().fine("ContextClient sendUpdate enter "+GUID+" json "+
				gnsAttrValuePairs);
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
				return;
			}
			
			sendUpdateToCS(GUID, 
					convertAttrValueToSystemFormat(csAttrValuePairs), 
					versionNum, blocking );
		} 
		catch (Exception | Error e)
		{
			e.printStackTrace();
		}
		// no waiting in update	
	}
	
	public int sendSearchQuery(String searchQuery, JSONArray replyArray, long expiryTime)
	{
		if( replyArray == null )
		{
			ContextServiceLogger.getLogger().warning("null passsed "
					+ "as replyArray in sendSearchQuery");
			return -1;
		}
		
		SearchReplyAnswer searchAnswer = sendSearchQueryToCS(searchQuery, expiryTime);
		
		// no untransformation needed in no privacy case.
		// context service returns an array of JSONArrays.
		// Each JSONArray contains the SearchReplyGUIDRepresentationJSON JSONObjects 
		for(int i=0; i<searchAnswer.resultArray.length(); i++)
		{
			try
			{
				JSONArray jsoArr1 = searchAnswer.resultArray.getJSONArray(i);
				for(int j=0; j<jsoArr1.length(); j++)
				{
					//resultGUIDMap.put(jsoArr1.getString(j), true);
					JSONObject searchRepJSON = jsoArr1.getJSONObject(j);
					SearchReplyGUIDRepresentationJSON searchRepObj = SearchReplyGUIDRepresentationJSON.fromJSONObject(searchRepJSON);
					replyArray.put(searchRepObj.getID());
				}
			} catch ( JSONException e )
			{
				e.printStackTrace();
			}
		}
		return searchAnswer.resultSize;
	}
	
	@Override
	public JSONObject sendGetRequest(String GUID) 
	{			
		long currId;
		synchronized(this.getIdLock)
		{
			currId = this.getReqId++;
		}
		
		GetMessage<NodeIDType> getmesgU 
			= new GetMessage<NodeIDType>(this.nodeid, currId, GUID, sourceIP, sourcePort);
		
		GetStorage<NodeIDType> getQ = new GetStorage<NodeIDType>();
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
	
	
	@Override
	/**
	 * blocking call to return the current triggers.
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
	
	@Override
	public void sendUpdateSecure( String GUID, GuidEntry myGUIDInfo, 
			JSONObject attrValuePairs, long versionNum, boolean blocking,
			HashMap<String, List<ACLEntry>> aclmap, List<AnonymizedIDEntry> anonymizedIDList )
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
			
			HashMap<String, AttrValueRepresentationJSON> attrValueMap =
					convertAttrValueToSystemFormat(csAttrValuePairs);
			
			// TODO: Different CSUpdateTransformedMessage messages can be computed
			// in parallel.
			
			long start1 = System.currentTimeMillis();
			
			List<CSUpdateTransformedMessage> transformedMesgList 
				= this.csPrivacyTransform.transformUpdateForCSPrivacy
				(myGUIDInfo.getGuid(), attrValueMap, aclmap, anonymizedIDList);
			
			long end1 = System.currentTimeMillis();
			
			// now all the anonymized IDs and the attributes that needs to be updated
			// are calculated, 
			// just need to send out updates\
			
			ParallelUpdateStateStorage updateState 
								= new ParallelUpdateStateStorage(transformedMesgList);
			
			
			for( int i=0;i<transformedMesgList.size();i++ )
			{
				CSUpdateTransformedMessage csTransformedMessage = transformedMesgList.get(i);
			
				UpdateOperationThread updateThread = new UpdateOperationThread(GUID, 
						csTransformedMessage, versionNum, blocking, 
						updateState );
				this.executorService.execute(updateThread);
				
				//String IDString = Utils.bytArrayToHex(csTransformedMessage.getAnonymizedID());
				
				//sendUpdateToCS( IDString, csTransformedMessage.getAttrValMap() , 
				//		versionNum, blocking );
			}
			updateState.waitForCompletion();
			
			long end2 = System.currentTimeMillis();
			
			System.out.println("Transform time "+(end1-start1)+" sending time "+(end2-end1)+
					" length "+attrValuePairs.length()+" transformedMesgList size "
					+transformedMesgList.size() );
		}
		catch( JSONException jsoEx )
		{
			jsoEx.printStackTrace();
		}
	}

	@Override
	public int sendSearchQuerySecure( String searchQuery, JSONArray replyArray, 
			long expiryTime, GuidEntry myGUIDInfo )
	{
		if( replyArray == null )
		{
			ContextServiceLogger.getLogger().warning("null passsed "
					+ "as replyArray in sendSearchQuery");
			return -1;
		}
		
		SearchReplyAnswer searchAnswer 
						= sendSearchQueryToCS(searchQuery, expiryTime);
		
		List<CSSearchReplyTransformedMessage> searchRepTransformList = 
				new LinkedList<CSSearchReplyTransformedMessage>();
		for(int i=0; i<searchAnswer.resultArray.length(); i++)
		{
			try
			{
				JSONArray jsoArr1 = searchAnswer.resultArray.getJSONArray(i);
				for(int j=0; j<jsoArr1.length(); j++)
				{
					//resultGUIDMap.put(jsoArr1.getString(j), true);
					JSONObject searchRepJSON = jsoArr1.getJSONObject(j);
					SearchReplyGUIDRepresentationJSON searchRepObj = SearchReplyGUIDRepresentationJSON.fromJSONObject(searchRepJSON);

					CSSearchReplyTransformedMessage csSearchRepTransform 
										= new CSSearchReplyTransformedMessage(searchRepObj);
					searchRepTransformList.add(csSearchRepTransform);
				}
			} catch ( JSONException e )
			{
				e.printStackTrace();
			}
		}
		
		this.csPrivacyTransform.unTransformSearchReply(myGUIDInfo,
				searchRepTransformList, replyArray);
		
		return searchAnswer.resultSize;
	}
	
	@Override
	public JSONObject sendGetRequestSecure(String GUID, GuidEntry myGUIDInfo) throws Exception
	{
		// fetch it form GNS
		if(gnsClient != null)
		{
			JSONObject encryptedJSON = gnsClient.read(GUID, myGUIDInfo);
			GNSTransformedMessage gnsTransformedMesg 
							= new GNSTransformedMessage(encryptedJSON);
			
			return this.gnsPrivacyTransform.unTransformGetReply(gnsTransformedMesg, myGUIDInfo);
		}
		return null;
	}
	
	/**
	 * assumption is that ACL always fits in memory.
	 */
	@Override
	public List<AnonymizedIDEntry> computeAnonymizedIDs(
			HashMap<String, List<ACLEntry>> aclMap) 
	{
		return this.anonymizedIDCreation.computeAnonymizedIDs(aclMap);
	}
	
	/**
	 * Sends update to CS in both privacy and non privacy case.
	 * filtering of CS attributes has already happened before this function call.
	 * It assumes all attributes in csAttrValMap are CS attributes.
	 */
	public void sendUpdateToCS( String GUID, 
			HashMap<String, AttrValueRepresentationJSON> csAttrValMap, 
			long versionNum, boolean blocking )
	{
		try
		{
			JSONObject jsonObject = ParsingMethods.getJSONObject(csAttrValMap);
			
			long currId;
			
			synchronized( this.updateIdLock )
			{
				currId = this.updateReqId++;
			}
			
			long reqeustID = currId;
			
			ValueUpdateFromGNS<NodeIDType> valUpdFromGNS = 
				new ValueUpdateFromGNS<NodeIDType>(null, versionNum, GUID, 
						jsonObject, reqeustID, sourceIP, sourcePort, 
							System.currentTimeMillis() );
			
			UpdateStorage<NodeIDType> updateQ = new UpdateStorage<NodeIDType>();
			updateQ.requestID = currId;
			//updateQ.valUpdFromGNS = valUpdFromGNS;
			updateQ.valUpdFromGNS = valUpdFromGNS;
			updateQ.valUpdFromGNSReply = null;
			updateQ.blocking = blocking;
			
			this.pendingUpdate.put(currId, updateQ);
			
			InetSocketAddress sockAddr = this.csNodeAddresses.get(rand.nextInt(csNodeAddresses.size()));
			
			ContextServiceLogger.getLogger().fine("ContextClient sending update requestID "+currId+" to "+sockAddr+" json "+
					valUpdFromGNS);		
			//niot.sendToAddress(sockAddr, valUpdFromGNS.toJSONObject());
			niot.sendToAddress(sockAddr, valUpdFromGNS.toJSONObject());
			
			if( blocking )
			{
				synchronized( updateQ )
				{
					while( updateQ.valUpdFromGNSReply == null )
					{
						try 
						{
							updateQ.wait();
						} catch (InterruptedException e) 
						{
							e.printStackTrace();
						}
					}
				}
				pendingUpdate.remove(currId);
			}
		}
		catch ( Exception | Error e )
		{
			e.printStackTrace();
		}
	}
	
	
	@Override
	public boolean handleMessage(JSONObject jsonObject)
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
				handleRefreshTrigger(jsonObject);
			} else if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE)
					== ContextServicePacket.PacketType.GET_REPLY_MESSAGE.getInt() )
			{
				handleGetReply(jsonObject);
			} else if(jsonObject.getInt(ContextServicePacket.PACKET_TYPE)
					== ContextServicePacket.PacketType.CONFIG_REPLY.getInt() )
			{
				handleConfigReply(jsonObject);
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		return true;
	}
	
	
	/**
	 * COnverts attr value JSON given in the api calls to
	 * attr AttrValueRepresentationJSON map
	 * @return
	 * @throws JSONException 
	 */
	private HashMap<String, AttrValueRepresentationJSON> convertAttrValueToSystemFormat
								(JSONObject attrValuePairs) throws JSONException
	{
		HashMap<String, AttrValueRepresentationJSON> map 
							= new HashMap<String, AttrValueRepresentationJSON>();
		
		Iterator<String> jsonIter = attrValuePairs.keys();
		while(jsonIter.hasNext())
		{
			String attrName = jsonIter.next();
			String value = attrValuePairs.getString(attrName);
			AttrValueRepresentationJSON valRep = new AttrValueRepresentationJSON(value);
			
			map.put(attrName, valRep);
		}
		return map;
	}
	
	private void sendUpdateToGNS(GuidEntry writingGuid, 
			JSONObject attrValuePair)
	{
		if(gnsClient != null)
		{
			try {
				gnsClient.update(writingGuid, attrValuePair);
			} catch (IOException | GnsClientException e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	private SearchReplyAnswer sendSearchQueryToCS(String searchQuery, long expiryTime)
	{	
		long currId;
		synchronized( this.searchIdLock )
		{
			currId = this.searchReqId++;
		}
		
		QueryMsgFromUser<NodeIDType> qmesgU 
			= new QueryMsgFromUser<NodeIDType>(this.nodeid, searchQuery, 
					currId, expiryTime, sourceIP, sourcePort);
		
		SearchQueryStorage<NodeIDType> searchQ = new SearchQueryStorage<NodeIDType>();
		searchQ.requestID = currId;
		searchQ.queryMsgFromUser = qmesgU;
		searchQ.queryMsgFromUserReply = null; 
		
		this.pendingSearches.put(currId, searchQ);
		InetSocketAddress sockAddr = this.csNodeAddresses.get(rand.nextInt(csNodeAddresses.size()));
		
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
		
		synchronized( searchQ )
		{
			while( searchQ.queryMsgFromUserReply == null )
			{
				try 
				{
					searchQ.wait();
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
		}
		
		JSONArray result = searchQ.queryMsgFromUserReply.getResultGUIDs();
		int resultSize = searchQ.queryMsgFromUserReply.getReplySize();
		pendingSearches.remove(currId);
		
		SearchReplyAnswer searchAnswer = new SearchReplyAnswer();
		searchAnswer.resultArray = result;
		searchAnswer.resultSize = resultSize;
		return searchAnswer;
	}
	
	/**
	 * Just used to return the search reply answer from
	 * private method to public method
	 * @author adipc
	 *
	 */
	private class SearchReplyAnswer 
	{
		JSONArray resultArray;
		int resultSize;
	}
	
	private void handleUpdateReply(JSONObject jso)
	{
		ValueUpdateFromGNSReply<NodeIDType> vur = null;
		try
		{
			vur = new ValueUpdateFromGNSReply<NodeIDType>(jso);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		long currReqID = vur.getUserReqNum();
		ContextServiceLogger.getLogger().fine("Update reply recvd "+currReqID);
		UpdateStorage<NodeIDType> replyUpdObj = this.pendingUpdate.get(currReqID);
		if( replyUpdObj != null )
		{
			if( replyUpdObj.blocking )
			{
				replyUpdObj.valUpdFromGNSReply = vur;
				
				synchronized(replyUpdObj)
				{
					replyUpdObj.notify();
				}
			}
			else
			{
				this.pendingUpdate.remove(currReqID);
			}
		}
		else
		{
			ContextServiceLogger.getLogger().fine("Update reply recvd "+currReqID+" update Obj null");
			assert(false);
		}
	}
	
	private void handleGetReply(JSONObject jso)
	{
		try
		{
			GetReplyMessage<NodeIDType> getReply;
			getReply = new GetReplyMessage<NodeIDType>(jso);
			
			long reqID = getReply.getReqID();
			GetStorage<NodeIDType> replyGetObj = this.pendingGet.get(reqID);
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
			ClientConfigReply<NodeIDType> configReply
									= new ClientConfigReply<NodeIDType>(jso);
			
			JSONArray nodeIPArray 		= configReply.getNodeConfigArray();
			JSONArray attrInfoArray 	= configReply.getAttributeArray();
			JSONArray subspaceInfoArray = configReply.getSubspaceInfoArray();
			
			for( int i=0;i<nodeIPArray.length();i++ )
			{
				String ipPort = nodeIPArray.getString(i);
				String[] parsed = ipPort.split(":");
				csNodeAddresses.add(new InetSocketAddress(parsed[0], Integer.parseInt(parsed[1])));
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
			
			synchronized( this.configLock )
			{
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
			QueryMsgFromUserReply<NodeIDType> qmur;
			qmur = new QueryMsgFromUserReply<NodeIDType>(jso);
			
			long reqID = qmur.getUserReqNum();
			//int resultSize = qmur.getReplySize();
			SearchQueryStorage<NodeIDType> replySearchObj = this.pendingSearches.get(reqID);
			replySearchObj.queryMsgFromUserReply = qmur;
			
			synchronized(replySearchObj)
			{
				replySearchObj.notify();
			}
			
//			r(int i=0;i<qmur.getResultGUIDs().length();i++)
//			{
//				resultSize = resultSize+qmur.getResultGUIDs().getJSONArray(i).length();
//			}
			//System.out.println("Search query completion requestID "+reqID+" time "+System.currentTimeMillis()+
			//		" replySize "+resultSize);
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	private void sendConfigRequest()
	{	
		ClientConfigRequest<NodeIDType> clientConfigReq 
			= new ClientConfigRequest<NodeIDType>(this.nodeid, sourceIP, sourcePort);
		
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
		
		synchronized( this.configLock )
		{
			while( csNodeAddresses.size() == 0 )
			{
				try 
				{
					this.configLock.wait();
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	private void handleRefreshTrigger(JSONObject jso)
	{
		try
		{
			RefreshTrigger<NodeIDType> qmur 
						= new RefreshTrigger<NodeIDType>(jso);
			
			synchronized(refreshTriggerClientWaitLock)
			{
				refreshTriggerQueue.add(qmur.toJSONObject());
				refreshTriggerClientWaitLock.notify();
			}
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
		
		// for anonymized ID
		anonymizedIDCreation = new SubspaceBasedAnonymizedIDCreator(subspaceAttrMap);
		
		// for gnsTransform
		gnsPrivacyTransform = new EncryptionBasedGNSPrivacyTransform();
		
		// for cs transform
		csPrivacyTransform = new SubspaceBasedCSTransform(executorService);
	}
	
	/**
	 * 
	 * Thread tha performs the update.
	 * @author adipc
	 */
	private class UpdateOperationThread implements Runnable
	{
		private final String guid;
		private final CSUpdateTransformedMessage csTransformedMessage;
		private final long versionNum;
		private final boolean blocking;
		private final ParallelUpdateStateStorage updateState;
		
		private final long startTime;
		public UpdateOperationThread(String guid, CSUpdateTransformedMessage csTransformedMessage
				, long versionNum, boolean blocking, ParallelUpdateStateStorage updateState)
		{
			this.guid = guid;
			this.csTransformedMessage = csTransformedMessage;
			this.versionNum = versionNum;
			this.blocking = blocking;
			this.updateState = updateState;
			startTime = System.currentTimeMillis();
		}
		
		@Override
		public void run() 
		{	
			String IDString = Utils.bytArrayToHex(csTransformedMessage.getAnonymizedID());
			
			sendUpdateToCS( IDString, csTransformedMessage.getAttrValMap() , 
					versionNum, blocking );
			
			long endTime = System.currentTimeMillis();
			if(ContextServiceConfig.DEBUG_MODE )
			{
				System.out.println("GUID "+guid+" AnonymizedID "+IDString+" update finished at "
							+(endTime-startTime));
			}
			updateState.incrementNumCompleted();
		}
	}
	
	// testing secure client code
	// TODO: test the scheme with the example given in the draft.
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
		
		System.out.println("privateKey0 "+((RSAPrivateKey)privateKey0).getModulus().bitLength());
		byte[] publicKeyByteArray0 = publicKey0.getEncoded();
		byte[] privateKeyByteArray0 = privateKey0.getEncoded();
		
		String guid0 = GuidUtils.createGuidFromPublicKey(publicKeyByteArray0);
		GuidEntry myGUID = new GuidEntry("Guid0", guid0, publicKey0, privateKey0);
		
//		PublicKey publicKey0 = masterGuid.getPublicKey();
//		PrivateKey privateKey0 = masterGuid.getPrivateKey();
//		byte[] publicKeyByteArray0 = publicKey0.getEncoded();
//		byte[] privateKeyByteArray0 = privateKey0.getEncoded();
//		
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
			byte[] privateKeyByteArray = privateKey.getEncoded();
			
			String guid = GuidUtils.createGuidFromPublicKey(publicKeyByteArray);
			
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
		
		
		ContextServiceClient<Integer> csClient = new ContextServiceClient<Integer>("127.0.0.1", 8000);
		
		List<AnonymizedIDEntry> anonymizedIdList = csClient.computeAnonymizedIDs(aclMap);
		JSONObject attrValPair = new JSONObject();
		attrValPair.put("attr1", 10+"");
		
		attrValPair.put("attr2", 15+"");
		
		csClient.sendUpdateSecure(guid0, myGUID, attrValPair, -1, true, aclMap, anonymizedIdList);
		
		Thread.sleep(2000);
		
		String searchQuery = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE attr1 >= 5 AND attr1 <= 15";
		JSONArray replyArray = new JSONArray();
		GuidEntry queryingGuid = guidsVector.get(3);
		csClient.sendSearchQuerySecure(searchQuery, replyArray, 300000, queryingGuid);
		
		System.out.println("Query for attr1 querying GUID "+ queryingGuid.getGuid()+
				" Real GUID "+guid0+" reply Arr "+replyArray);
		
		
		searchQuery = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE attr1 >= 5 AND attr1 <= 15"
				+ " AND attr2 >= 10 AND attr2 <= 20";
		replyArray = new JSONArray();
		queryingGuid = guidsVector.get(1);
		csClient.sendSearchQuerySecure(searchQuery, replyArray, 300000, queryingGuid);
		
		System.out.println("Query for att1 and attr4 querying GUID "+ queryingGuid.getGuid()+
				" Real GUID "+guid0+" reply Arr "+replyArray);
		
//		queryingGuid = guidsVector.get(1);
//		JSONObject getObj = csClient.sendGetRequestSecure(guid0, queryingGuid);
//		System.out.println("recvd Obj "+getObj);
	}
}