package edu.umass.cs.contextservice.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Vector;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.anonymizedID.AnonymizedIDCreationInterface;
import edu.umass.cs.contextservice.client.anonymizedID.NoopAnonymizedIDCreator;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.client.common.GUIDEntryStoringClass;
import edu.umass.cs.contextservice.client.csprivacytransform.CSPrivacyTransformInterface;
import edu.umass.cs.contextservice.client.csprivacytransform.CSTransformedUpdatedMessage;
import edu.umass.cs.contextservice.client.csprivacytransform.NoopCSTransform;
import edu.umass.cs.contextservice.client.gnsprivacytransform.GNSPrivacyTransformInterface;
import edu.umass.cs.contextservice.client.gnsprivacytransform.GNSTransformedUpdateMessage;
import edu.umass.cs.contextservice.client.gnsprivacytransform.NoopGNSPrivacyTransform;
import edu.umass.cs.contextservice.client.storage.GetStorage;
import edu.umass.cs.contextservice.client.storage.SearchQueryStorage;
import edu.umass.cs.contextservice.client.storage.UpdateStorage;
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
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClientExtended;
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
												implements SecureContextClientInterface
{
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
	/**
	 * Use this constructor if you want to directly communicate with CS, bypassing GNS.
	 * @param csHostName
	 * @param csPortNum
	 * @throws IOException
	 */
	public ContextServiceClient(String csHostName, int csPortNum) throws IOException
	{
		super( csHostName, csPortNum );
		gnsClient = null;
		
		initializeClient();
	}
	
	
	public ContextServiceClient(String csHostName, int csPortNum, String gnsHostName, int gnsPort) 
			throws IOException
	{
		super( csHostName, csPortNum );
		
		// just setting some gns properties.
		Properties props = System.getProperties();
		props.setProperty("gigapaxosConfig", "conf/gnsClientConf/gigapaxos.client.local.properties");
		props.setProperty("javax.net.ssl.trustStorePassword", "qwerty");
		props.setProperty("javax.net.ssl.trustStore", "conf/gnsClientConf/trustStore/node100.jks");
		props.setProperty("javax.net.ssl.keyStorePassword", "qwerty");
		props.setProperty("javax.net.ssl.keyStore", "conf/gnsClientConf/keyStore/node100.jks");	
		
		InetSocketAddress address 
					= new InetSocketAddress("127.0.0.1", GNSClientConfig.LNS_PORT);
		
		gnsClient = new GNSClient(null, address, true);
		
		initializeClient();
	}
	
	
	@Override
	public void sendUpdate( String GUID, JSONObject gnsAttrValuePairs, 
			long versionNum, boolean blocking )
	{
		ContextServiceLogger.getLogger().fine("ContextClient sendUpdate enter "+GUID+" json "+
				gnsAttrValuePairs);
		try
		{
			long currId;
			
			synchronized( this.updateIdLock )
			{
				currId = this.updateReqId++;
			}
			
			JSONObject csAttrValuePairs = filterCSAttributes(gnsAttrValuePairs);
			
			// no context service attribute matching.
			if( csAttrValuePairs.length() <= 0 )
			{
				return;
			}
			
			long reqeustID = currId;
			
			ValueUpdateFromGNS<NodeIDType> valUpdFromGNS = 
					new ValueUpdateFromGNS<NodeIDType>(null, versionNum, GUID, csAttrValuePairs, reqeustID, sourceIP, sourcePort, 
							System.currentTimeMillis() , new JSONObject());
			
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
		} catch (JSONException e)
		{
			e.printStackTrace();
		} catch ( UnknownHostException e )
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		catch(Exception | Error ex)
		{
			ex.printStackTrace();
		}
		// no waiting in update	
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
		
		long currId;
		synchronized( this.searchIdLock )
		{
			currId = this.searchReqId++;
		}
		
		QueryMsgFromUser<NodeIDType> qmesgU 
			= new QueryMsgFromUser<NodeIDType>(this.nodeid, searchQuery, currId, expiryTime, sourceIP, sourcePort);
		
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
		
		// convert result to list of GUIDs, currently is is list of JSONArrays 
		//JSONArray resultRet = new JSONArray();
		for(int i=0; i<result.length(); i++)
		{
			try
			{
				JSONArray jsoArr1 = result.getJSONArray(i);
				for(int j=0; j<jsoArr1.length(); j++)
				{
					//resultGUIDMap.put(jsoArr1.getString(j), true);
					replyArray.put(jsoArr1.getString(j));
				}
			} catch ( JSONException e )
			{
				e.printStackTrace();
			}
		}
		return resultSize;
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
	
//	@Override
//	public void expireSearchQuery(String searchQuery) 
//	{
//	}
	
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
	public void sendUpdateSecure(GUIDEntryStoringClass myGUIDInfo, 
			HashMap<String, List<ACLEntry>> aclMap, List<AnonymizedIDEntry> anonymizedIDList, 
			JSONObject gnsAttrValuePairs, long versionNum, boolean blocking)
	{
		try
		{
			JSONObject csAttrValuePairs = filterCSAttributes(gnsAttrValuePairs);
			// no context service attribute matching.
			if( csAttrValuePairs.length() <= 0 )
			{
				return;
			}
			
			//TODO: send secure update to GNS
			GNSTransformedUpdateMessage gnsTransformMesg = 
					this.gnsPrivacyTransform.transformUpdateForGNSPrivacy(gnsAttrValuePairs, aclMap);
			
			this.sendSecureMessageToGNS(null, gnsTransformMesg);
			
			List<CSTransformedUpdatedMessage> transformedMesgList 
			= this.csPrivacyTransform.transformUpdateForCSPrivacy
			(Utils.bytArrayToHex(myGUIDInfo.getGuidByteArray()), csAttrValuePairs, aclMap, anonymizedIDList);
			
			// now all the anonymized IDs and the attributes that needs to be updated
			// are calculated, 
			// just need to send out updates
			
			sendSecureUpdateMessageToCS(transformedMesgList, versionNum, blocking);
		}
		catch( JSONException jsoEx )
		{
			jsoEx.printStackTrace();
		}
	}

	@Override
	public int sendSearchQuerySecure(GUIDEntryStoringClass myGUIDInfo, String searchQuery, 
			JSONArray replyArray, long expiryTime) 
	{
		if( replyArray == null )
		{
			ContextServiceLogger.getLogger().warning("null passsed "
					+ "as replyArray in sendSearchQuery");
			return -1;
		}
		
		long currId;
		synchronized( this.searchIdLock )
		{
			currId = this.searchReqId++;
		}
		
		QueryMsgFromUser<NodeIDType> qmesgU 
			= new QueryMsgFromUser<NodeIDType>(this.nodeid, searchQuery, currId, 
					expiryTime, sourceIP, sourcePort);
		
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
		JSONArray encryptedRealIDArray = searchQ.queryMsgFromUserReply.getEncryptedRealIDArray();
		
		pendingSearches.remove(currId);
		
		// convert result to list of GUIDs, currently is is list of JSONArrays 
		//JSONArray resultRet = new JSONArray();
		/*for(int i=0; i<result.length(); i++)
		{
			try
			{
				JSONArray jsoArr1 = result.getJSONArray(i);
				for(int j=0; j<jsoArr1.length(); j++)
				{
					//resultGUIDMap.put(jsoArr1.getString(j), true);
					replyArray.put(jsoArr1.getString(j));
				}
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}*/
		
		//TODO: untransform
//		decryptAnonymizedIDs(myGUIDInfo, 
//				encryptedRealIDArray, replyArray);
		
		return resultSize;
	}
	
	@Override
	public JSONObject sendGetRequestSecure(GUIDEntryStoringClass myGUIDInfo, String GUID)
	{
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
	
	private void sendSecureUpdateMessageToCS(List<CSTransformedUpdatedMessage> transformedMesgList, 
			long versionNum, boolean blocking)
	{
		for(int i=0;i<transformedMesgList.size();i++)
		{
			CSTransformedUpdatedMessage csTransformedMessage = transformedMesgList.get(i);
			
			String IDString = Utils.bytArrayToHex(csTransformedMessage.getAnonymizedID());
	//		ContextServiceLogger.getLogger().fine("ContextClient sendUpdate enter "+GUID+" json "+
	//				gnsAttrValuePairs);
			try
			{
				long currId;
				
				synchronized( this.updateIdLock )
				{
					currId = this.updateReqId++;
				}
				
				long reqeustID = currId;
				
				ValueUpdateFromGNS<NodeIDType> valUpdFromGNS = 
						new ValueUpdateFromGNS<NodeIDType>(null, versionNum, IDString, 
								csTransformedMessage.getAttrValuePairJSON(), reqeustID, sourceIP, sourcePort, 
								System.currentTimeMillis() , csTransformedMessage.getRealIDMappingInfoJSON());
				
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
			} catch (JSONException e)
			{
				e.printStackTrace();
			} catch ( UnknownHostException e )
			{
				e.printStackTrace();
			} catch (IOException e)
			{
				e.printStackTrace();
			}
			catch(Exception | Error ex)
			{
				ex.printStackTrace();
			}
		}
		// no waiting in update	
	}
	
	private void sendSecureMessageToGNS(GuidEntry writingGuid, GNSTransformedUpdateMessage gnsTransformedMesg)
	{
		if(gnsClient != null)
		{
			try {
				gnsClient.update(writingGuid, gnsTransformedMesg.getEncryptedAttrValuePair());
			} catch (IOException | GnsClientException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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
	
	private void initializeClient()
	{
		// for anonymized ID
		anonymizedIDCreation = new NoopAnonymizedIDCreator();
		
		// for gnsTransform
		gnsPrivacyTransform = new NoopGNSPrivacyTransform();
		
		// for cs transform
		csPrivacyTransform = new NoopCSTransform();
		
		refreshTriggerQueue = new LinkedList<JSONObject>();
		// FIXME: add a timeout mechanism here.
		sendConfigRequest();
	}
	
	
	
	// testing secure client code
	// TODO: test the scheme with the example given in the draft.
	public static void main(String[] args) throws JSONException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException, IOException
	{
		KeyPairGenerator kpg;
		kpg = KeyPairGenerator.getInstance("RSA");
		kpg.initialize(1024);
		KeyPair kp1 = kpg.genKeyPair();
		Key publicKey1 = kp1.getPublic();
		Key privateKey1 = kp1.getPrivate();
		byte[] publicKeyByteArray1 = publicKey1.getEncoded();
		byte[] privateKeyByteArray1 = privateKey1.getEncoded();
		
		KeyPair kp2 = kpg.genKeyPair();
		Key publicKey2 = kp2.getPublic();
		Key privateKey2 = kp2.getPrivate();
		byte[] publicKeyByteArray2 = publicKey2.getEncoded();
		byte[] privateKeyByteArray2 = privateKey2.getEncoded();
		
		
		String info = "name";
		byte[] encryptedName = Utils.doPublicKeyEncryption(publicKeyByteArray1, info.getBytes());
		byte[] nameArray = Utils.doPrivateKeyDecryption(privateKeyByteArray1, encryptedName);
		System.out.println("decrypted value "+new String(nameArray));
		
		// wrong private key decryption
		// on bad decryption this javax.crypto.BadPaddingException is thrown
//		nameArray = Utils.doPrivateKeyDecryption(privateKeyByteArray2, encryptedName);
//		System.out.println("decrypted value "+new String(nameArray));
		
		
			
		System.out.println("public key "+Utils.bytArrayToHex(publicKeyByteArray1)
			+" len "+publicKeyByteArray1.length+" privateKeyByteArray "
			+ Utils.bytArrayToHex(privateKeyByteArray1) 
			+" len "+privateKeyByteArray1.length);
		
		//testing JSON put of byte array
		JSONObject jsonObject = new JSONObject();
		jsonObject.put( GUIDEntryStoringClass.PUBLICKEY_KEY 
				, publicKeyByteArray1);
		
		byte[] fetchedByteArr = 
				(byte[]) jsonObject.get(GUIDEntryStoringClass.PUBLICKEY_KEY);
		
		System.out.println("fetched public key "+Utils.bytArrayToHex(fetchedByteArr)
		+" len "+fetchedByteArr.length);
		
		
		JSONArray testAtrr1 = new JSONArray();
		JSONArray testAtrr2 = new JSONArray();
		HashMap<JSONArray, Integer> testMap = new HashMap<JSONArray, Integer>();
		testAtrr1.put("a1");
		testAtrr1.put("a2");
		
		testAtrr2.put("a1");
		testAtrr2.put("a2");
		
		testMap.put(testAtrr1, 1);
		testMap.put(testAtrr2, 2);
		
		Iterator<JSONArray> iter =  testMap.keySet().iterator();
		
		while( iter.hasNext() )
		{
			JSONArray key = iter.next();
			System.out.println("key "+key+" "+testMap.get(key));
		}
		
		
		
		Vector<GUIDEntryStoringClass> guidsVector = new Vector<GUIDEntryStoringClass>();
		GUIDEntryStoringClass myGUID = new GUIDEntryStoringClass(
				Utils.convertPublicKeyToGUIDByteArray(publicKeyByteArray1), publicKeyByteArray1 ,
				privateKeyByteArray1);
		
		guidsVector.add(myGUID);
		
		// draft example has 7 guids
		for(int i=1; i <= 7; i++)
		{
			KeyPair kp = kpg.genKeyPair();
			Key publicKey = kp.getPublic();
			Key privateKey = kp.getPrivate();
			byte[] publicKeyByteArray = publicKey.getEncoded();
			byte[] privateKeyByteArray = privateKey.getEncoded();
			
			GUIDEntryStoringClass currGUID = new GUIDEntryStoringClass(
					Utils.convertPublicKeyToGUIDByteArray(publicKeyByteArray), publicKeyByteArray ,
					privateKeyByteArray);
			
			guidsVector.add(currGUID);
		}
		
		
		//FIXME: fix the testing code.
	/*	// more testing of each method in secure interface.
		// test with the example in the draft.
		ContextServiceClient<Integer> csClient = new ContextServiceClient<Integer>("127.0.0.1", 8000);
		//GUIDEntryStoringClass myGUIDInfo, JSONArray ACLArray
		
		List<byte[]> acl0 = new LinkedList<byte[]>();
		acl0.add(guidsVector.get(1).getPublicKeyByteArray());
		acl0.add(guidsVector.get(2).getPublicKeyByteArray());
		acl0.add(guidsVector.get(3).getPublicKeyByteArray());
		ACLEntry attr0ACL = new ACLEntry("attr0", acl0);
		
		
		List<byte[]> acl1 = new LinkedList<byte[]>();
		acl1.add(guidsVector.get(4).getPublicKeyByteArray());
		acl1.add(guidsVector.get(5).getPublicKeyByteArray());
		acl1.add(guidsVector.get(1).getPublicKeyByteArray());
		ACLEntry attr1ACL = new ACLEntry("attr1", acl1);
		
		
		List<byte[]> acl2 = new LinkedList<byte[]>();
		acl2.add(guidsVector.get(1).getPublicKeyByteArray());
		acl2.add(guidsVector.get(2).getPublicKeyByteArray());
		ACLEntry attr2ACL = new ACLEntry("attr2", acl2);
		
		
		List<byte[]> acl3 = new LinkedList<byte[]>();
		acl3.add(guidsVector.get(1).getPublicKeyByteArray());
		acl3.add(guidsVector.get(2).getPublicKeyByteArray());
		acl3.add(guidsVector.get(3).getPublicKeyByteArray());
		ACLEntry attr3ACL = new ACLEntry("attr3", acl3);
		
		
		List<byte[]> acl4 = new LinkedList<byte[]>();
		acl4.add(guidsVector.get(6).getPublicKeyByteArray());
		acl4.add(guidsVector.get(7).getPublicKeyByteArray());
		ACLEntry attr4ACL = new ACLEntry("attr4", acl4);
		
		
		List<byte[]> acl5 = new LinkedList<byte[]>();
		acl5.add(guidsVector.get(4).getPublicKeyByteArray());
		acl5.add(guidsVector.get(5).getPublicKeyByteArray());
		acl5.add(guidsVector.get(3).getPublicKeyByteArray());
		
		ACLEntry attr5ACL = new ACLEntry("attr5", acl5);
		
		
		
		JSONArray ACLInfo = new JSONArray();
		ACLInfo.put(attr0ACL);
		ACLInfo.put(attr1ACL);
		ACLInfo.put(attr2ACL);
		ACLInfo.put(attr3ACL);
		ACLInfo.put(attr4ACL);
		ACLInfo.put(attr5ACL);
		
		JSONArray anonymizedIds = csClient.computeAnonymizedIDs(myGUID, ACLInfo);
		System.out.println("Number of anonymizedIds "+anonymizedIds.length());*/
	}
}