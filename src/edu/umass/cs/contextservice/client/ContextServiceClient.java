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
import java.util.Queue;
import java.util.Random;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

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
	
	private final Random anonymizedIDRand							= new Random();
	
	public ContextServiceClient(String hostName, int portNum) throws IOException
	{
		super( hostName, portNum );
		refreshTriggerQueue = new LinkedList<JSONObject>();
		// FIXME: add a timeout mechanism here.
		sendConfigRequest();
	}
	
	@Override
	public void sendUpdate( String GUID, JSONObject gnsAttrValuePairs, long versionNum, boolean blocking )
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
			} catch (JSONException e)
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
	
	@Override
	public void sendUpdateSecure(GUIDEntryStoringClass myGUIDInfo, JSONArray ACLArray, JSONArray anonymizedIDs, 
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
			
			// a map is computed that contains anonymized IDs whose Gul, guid set, intersects with
			// the ACL of the attributes. The map is from the anonymized IDs to the set of attributes
			// that needs to be updated for that anonymized ID, this set is subset of attrs in attrValuePairs json.
			
			HashMap<byte[], List<AttributeUpdateInfo>> anonymizedIDToAttributesMap 
											= new HashMap<byte[], List<AttributeUpdateInfo>>();
			
			Iterator<String> updatedAttrIter = csAttrValuePairs.keys();
			while( updatedAttrIter.hasNext() )
			{
				String updAttr = updatedAttrIter.next();
				HashMap<String, ACLStoringClass> attrACLMap 
									= convertACLFromJSONArrayToMap(ACLArray);
				
				
				for( int i=0;i<anonymizedIDs.length();i++ )
				{
					AnonymizedIDStoringClass anonymizedID 
									= (AnonymizedIDStoringClass)anonymizedIDs.get(i);
					
					JSONArray intersectioPublicKeyMembers 
					= computeTheInsectionOfACLAndAnonymizedIDGuidSet(attrACLMap.get(updAttr).getPublicKeyACLMembers(),
							anonymizedID.getGUIDSet() );
					
					if( intersectioPublicKeyMembers.length() > 0 )
					{
						List<AttributeUpdateInfo> attrUpdateList 
								= anonymizedIDToAttributesMap.get(anonymizedID.getID());
						
						if( attrUpdateList == null )
						{
							attrUpdateList = new LinkedList<AttributeUpdateInfo>();
							AttributeUpdateInfo attrUpdObj = new AttributeUpdateInfo(updAttr, intersectioPublicKeyMembers);
							attrUpdateList.add(attrUpdObj);
							anonymizedIDToAttributesMap.put(anonymizedID.getID(), attrUpdateList);		
						}
						else
						{
							AttributeUpdateInfo attrUpdObj = new AttributeUpdateInfo(updAttr, intersectioPublicKeyMembers);
							attrUpdateList.add(attrUpdObj);
						}
					}
				}
			}
			
			// now all the anonymized IDs and the attributes that needs to be updated
			// are calculated, 
			// just need to send out updates
			//HashMap<byte[], List<AttributeUpdateInfo>> anonymizedIDToAttributesMap 
			// = new HashMap<byte[], List<AttributeUpdateInfo>>();
			Iterator<byte[]> anonymizedIter = anonymizedIDToAttributesMap.keySet().iterator();
			
			while( anonymizedIter.hasNext() )
			{
				byte[] anonymizedID 	= anonymizedIter.next();
				List<AttributeUpdateInfo> updateAttrList 
										= anonymizedIDToAttributesMap.get(anonymizedID);
				
				sendSecureUpdateMessageToCS(anonymizedID, myGUIDInfo.getGuidByteArray(), 
						updateAttrList, csAttrValuePairs, versionNum, blocking);
			}
		}
		catch( JSONException jsoEx )
		{
			jsoEx.printStackTrace();
		}
	}
	
	/**
	 * First argument is public key, second is GUID. need to convert that to match.
	 * @param publicKeyACLMembers
	 * @return null if no intersection
	 * @throws JSONException 
	 */
	private JSONArray computeTheInsectionOfACLAndAnonymizedIDGuidSet
		(JSONArray publicKeyACLMembers, JSONArray guidSetOfAnonymizedID) throws JSONException
	{
		JSONArray intersection = new JSONArray();
		
		HashMap<byte[], Boolean> guidMapForAnonymizedID = new HashMap<byte[], Boolean>();
		
		for( int i=0; i < guidSetOfAnonymizedID.length(); i++)
		{
			byte[] guid = (byte[]) guidSetOfAnonymizedID.get(i);
			guidMapForAnonymizedID.put(guid, true);
		}
		
		for(int i=0; i<publicKeyACLMembers.length(); i++)
		{
			byte[] publicKeyByteArray = (byte[]) publicKeyACLMembers.get(i);
			byte[] guid = Utils.convertPublicKeyToGUIDByteArray(publicKeyByteArray);
			
			// intersection
			if( guidMapForAnonymizedID.containsKey(guid) )
			{
				intersection.put(guid);
			}
		}
		return intersection;
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
		decryptAnonymizedIDs(myGUIDInfo, 
				encryptedRealIDArray, replyArray);
		
		return resultSize;
	}
	
	/**
	 * Decrypts the anonymized IDs .
	 * The result is returned in replyArray.
	 * @param myGUIDInfo
	 * @param encryptedRealIDArray
	 * @param replyArray
	 * @return
	 */
	private void decryptAnonymizedIDs(GUIDEntryStoringClass myGUIDInfo, 
			JSONArray encryptedRealIDArray, JSONArray replyArray)
	{
		for( int i=0; i<encryptedRealIDArray.length(); i++ )
		{
			try
			{
				JSONObject currJSON = encryptedRealIDArray.getJSONObject(i);
				Iterator<String> anonymizedIDIter = currJSON.keys();
				
				while( anonymizedIDIter.hasNext() )
				{
					String anonymizedIDHex = anonymizedIDIter.next();
					// each element is byte[], 
					// convert it to String, then to JSONArray.
					// each element of JSONArray is encrypted RealID.
					// each element needs to be decrypted with private key.
					
					byte[] jsonArrayByteArray = (byte[]) currJSON.get(anonymizedIDHex);
					// this operation can be inefficient
					String jsonArrayString = new String(jsonArrayByteArray);
					
					JSONArray jsonArray = new JSONArray(jsonArrayString);
					
					byte[] resultGUIDBytes = decryptTheRealID( myGUIDInfo, jsonArray );
					
					replyArray.put(new String(resultGUIDBytes));
				}
			} catch ( JSONException e )
			{
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * this function decrypts and returns the real ID from json Array of encrypted values.
	 * @param myGUIDInfo
	 * @param encryptedRealJsonArray
	 * @return
	 * @throws JSONException 
	 */
	private byte[] decryptTheRealID( GUIDEntryStoringClass myGUIDInfo, 
			JSONArray encryptedRealJsonArray ) throws JSONException
	{
		byte[] privateKey = myGUIDInfo.getPrivateKeyByteArray();
		byte[] plainText = null;
		boolean found = false;
		for( int i=0; i<encryptedRealJsonArray.length(); i++ )
		{
			byte[] encryptedElement = (byte[]) encryptedRealJsonArray.get(i);
			
			try
			{
				plainText = Utils.doPrivateKeyDecryption(privateKey, encryptedElement);
				// non exception, just break;
				break;
			}
			catch(javax.crypto.BadPaddingException wrongKeyException)
			{
				// just catching this one, as this one results when wrong key is used 
				// to decrypt.
			} catch (InvalidKeyException e) 
			{
				e.printStackTrace();
			} catch (NoSuchAlgorithmException e) 
			{
				e.printStackTrace();
			} catch (InvalidKeySpecException e) 
			{
				e.printStackTrace();
			} catch (NoSuchPaddingException e) 
			{
				e.printStackTrace();
			} catch (IllegalBlockSizeException e) 
			{
				e.printStackTrace();
			}
		}
		assert(plainText != null);
		return plainText;
	}
	
	@Override
	public JSONObject sendGetRequestSecure(GUIDEntryStoringClass myGUIDInfo, String GUID)
	{
		return null;
	}
	
	private HashMap<String, ACLStoringClass> convertACLFromJSONArrayToMap(JSONArray ACLArray) throws JSONException
	{
		HashMap<String, ACLStoringClass> attrACLMap 
					= new HashMap<String, ACLStoringClass>();
		
		for( int i=0; i<ACLArray.length() ; i++ )
		{
			ACLStoringClass currAttACL 
				= (ACLStoringClass)ACLArray.get(i);
			
			attrACLMap.put(currAttACL.getAttrName(), currAttACL);
		}
		return attrACLMap;
	}
	
	/**
	 * 
	 * assumption is that ACL always fits in memory.
	 */
	@Override
	public JSONArray computeAnonymizedIDs(
			GUIDEntryStoringClass myGUIDInfo, JSONArray ACLArray) 
	{
		//TODO: Testing pending
		try
		{
			// each element is AnonymizedIDStoringClass
			JSONArray anonymizedIDArray = new JSONArray();
			
			HashMap<String, ACLStoringClass> attrACLMap 
							= convertACLFromJSONArrayToMap(ACLArray);
			
			
			Iterator<Integer> subspaceAttrIter 
										= this.subspaceAttrMap.keySet().iterator();
			
			while( subspaceAttrIter.hasNext() )
			{
				int mapKey = subspaceAttrIter.next();
				
				JSONArray attrArray = this.subspaceAttrMap.get(mapKey);
				
				// byte[] is GUID
				HashMap<byte[], List<String>> guidToAttributesMap 
								= new HashMap<byte[], List<String>>();
				
				for( int i=0; i<attrArray.length(); i++ )
				{
					String currAttr = attrArray.getString(i);
					
					ACLStoringClass attrACL = attrACLMap.get(currAttr);
					JSONArray publicKeyACLMembers = attrACL.getPublicKeyACLMembers();
					
					for( int j=0; j<publicKeyACLMembers.length(); j++ )
					{
						byte[] publicKeyByteArray = (byte[]) publicKeyACLMembers.get(j);
						byte[] guidByteArray = Utils.convertPublicKeyToGUIDByteArray(publicKeyByteArray);
						
						List<String> attrListBuk = guidToAttributesMap.get(guidByteArray);
						
						if( attrListBuk == null )
						{
							attrListBuk = new LinkedList<String>();
							attrListBuk.add(currAttr);
							guidToAttributesMap.put(guidByteArray, attrListBuk);
						}
						else
						{
							attrListBuk.add(currAttr);
						}
					}
				}
				
				
				// map computed now compute anonymized IDs
				// we sort the list of attributes, so that different permutations of same set 
				// becomes same and then just add them to hashmap for finding distinct sets.
				
				HashMap<JSONArray, JSONArray> attributesToGuidsMap 
											= new HashMap<JSONArray, JSONArray>();
				
				Iterator<byte[]> guidIter = guidToAttributesMap.keySet().iterator();
				
				while( guidIter.hasNext() )
				{
					byte[] currGUID = guidIter.next();
					List<String> attrListBuk = guidToAttributesMap.get(currGUID);
					// sorting by natural String order
					attrListBuk.sort(null);
					
					// it is just concantenation of attrs in sorted order
					JSONArray attrArrayBul = new JSONArray();
					for( int i=0; i < attrListBuk.size(); i++)
					{
						attrArrayBul.put(attrListBuk.get(i));
					}
					
					JSONArray guidsListArray = attributesToGuidsMap.get(attrArrayBul);
					
					if( guidsListArray == null )
					{
						guidsListArray = new JSONArray();
						guidsListArray.put(currGUID);
						attributesToGuidsMap.put(attrArrayBul, guidsListArray);
					}
					else
					{
						guidsListArray.put(currGUID);
					}
				}
				
				// now assign anonymized ID
				
				//HashMap<String, List<byte[]>> attributesToGuidsMap 
				//	= new HashMap<String, List<byte[]>>();

				Iterator<JSONArray> attrSetIter = attributesToGuidsMap.keySet().iterator();
				
				while( attrSetIter.hasNext() )
				{
					JSONArray attrSet = attrSetIter.next();
					JSONArray guidSet = attributesToGuidsMap.get(attrSet);
					
					byte[] anonymizedID 
								= new byte[SecureContextClientInterface.SIZE_OF_ANONYMIZED_ID];
					
					anonymizedIDRand.nextBytes(anonymizedID);
					
					AnonymizedIDStoringClass anonymizedIDObj 
						= new AnonymizedIDStoringClass(anonymizedID, attrSet, guidSet);
					anonymizedIDArray.put(anonymizedIDObj);
				}
			}	
			return anonymizedIDArray;
		}
		catch( JSONException jsoEx )
		{
			jsoEx.printStackTrace();
		}
		return null;
	}
	
	/**
	 * This function add attr:value pair in JSONObject, 
	 * Value is of the form ValueClassInPrivacy.
	 * This function also encrypts anonymized ID by encrypting with 
	 * ACL members public key.
	 * This may change, if in future we change it to a multi recipient secret 
	 * sharing.
	 * @param csAttrValuePairsSec
	 * @param attrUpdInfo
	 * @param value
	 * @throws JSONException 
	 */
	private void addSecureValueInJSON(byte[] realGUID, JSONObject csAttrValuePairs, 
			JSONObject encryptedRealIDPair, AttributeUpdateInfo attrUpdInfo, String value) throws JSONException
	{
		JSONArray encryptedRealIDArray = new JSONArray();
		JSONArray publicKeyArray = attrUpdInfo.publicKeyArray;
		
		for( int i=0; i<publicKeyArray.length(); i++ )
		{
			// catching here so that one bad key doesn't let everything else to fail.
			try 
			{
				byte[] publicKey = (byte[]) publicKeyArray.get(i);
				byte[] encryptedRealID = 
						 Utils.doPublicKeyEncryption(publicKey, realGUID);
				
				encryptedRealIDArray.put(encryptedRealID);
				
			} catch (NoSuchAlgorithmException e) 
			{
				e.printStackTrace();
			} catch (InvalidKeySpecException e) 
			{
				e.printStackTrace();
			} catch (JSONException e) 
			{
				e.printStackTrace();
			} catch (InvalidKeyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchPaddingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalBlockSizeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (BadPaddingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		//ValueClassInPrivacy valueObj = new ValueClassInPrivacy(value, encryptedRealIDArray);
		// add the above value in the JSON
		csAttrValuePairs.put(attrUpdInfo.getAttrName(), value);
		
		encryptedRealIDPair.put(attrUpdInfo.getAttrName(), encryptedRealIDArray);
	}
	
	private void sendSecureUpdateMessageToCS(byte[] anonymizedID, byte[] realGUID, 
			List<AttributeUpdateInfo> updateAttrList, JSONObject csAttrValuePairs, long versionNum, boolean blocking)
	{
		String IDString = Utils.bytArrayToHex(anonymizedID);
//		ContextServiceLogger.getLogger().fine("ContextClient sendUpdate enter "+GUID+" json "+
//				gnsAttrValuePairs);
		try
		{
			long currId;
			
			synchronized( this.updateIdLock )
			{
				currId = this.updateReqId++;
			}
			
			// no context service attribute matching.
			if( updateAttrList.size() <= 0 )
			{
				assert( false );
			}
			
			JSONObject csAttrValuePairsSec = new JSONObject();
			JSONObject encryptedRealIDPair = new JSONObject();
			
			for( int i=0; i<updateAttrList.size(); i++ )
			{
				AttributeUpdateInfo attrUpdInfo = updateAttrList.get(i);
				
				// getting the updated value from the user supplied JSON.
				String value = csAttrValuePairs.getString(attrUpdInfo.getAttrName());
				addSecureValueInJSON( realGUID, csAttrValuePairsSec, encryptedRealIDPair, 
						attrUpdInfo, value );
			}		
			
			long reqeustID = currId;
			
			ValueUpdateFromGNS<NodeIDType> valUpdFromGNS = 
					new ValueUpdateFromGNS<NodeIDType>(null, versionNum, IDString, 
							csAttrValuePairsSec, reqeustID, sourceIP, sourcePort, 
							System.currentTimeMillis() , encryptedRealIDPair);
			
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
	
	/**
	 * This class stores attribute info on an update 
	 * for each anonymized ID.
	 * 
	 * @author adipc
	 */
	private class AttributeUpdateInfo
	{
		private final String attrName;
		
		// this is the array of public key members that are common in this 
		// attribute's ACL and the corresponding anonymized IDs guid set, Gul.
		// this array is used to encrypt the real ID of the user. 
		private final JSONArray publicKeyArray;
		
		public AttributeUpdateInfo(String attrName, JSONArray publicKeyArray)
		{
			this.attrName = attrName;
			this.publicKeyArray = publicKeyArray;
		}
		
		public String getAttrName()
		{
			return this.attrName;
		}
		
		public JSONArray getPublicKeyArray()
		{
			return this.publicKeyArray;
		}
	}
	
	
	// testing secure client code
	// TODO: test the scheme with the example given in the draft.
	public static void main(String[] args) throws JSONException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException
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
		nameArray = Utils.doPrivateKeyDecryption(privateKeyByteArray2, encryptedName);
		System.out.println("decrypted value "+new String(nameArray));
		
		
			
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
		
		testAtrr2.put("a2");
		testAtrr2.put("a1");
		
		testMap.put(testAtrr1, 1);
		testMap.put(testAtrr2, 2);
		
		Iterator<JSONArray> iter =  testMap.keySet().iterator();
		
		while( iter.hasNext() )
		{
			JSONArray key = iter.next();
			System.out.println("key "+key+" "+testMap.get(key));
		}
		
		// more testing of each method in secure interface.
		// test with the example in the draft.
		
		
		
	}
}