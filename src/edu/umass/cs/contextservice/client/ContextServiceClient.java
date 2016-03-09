package edu.umass.cs.contextservice.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

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
{
	private Queue<JSONObject> refreshTriggerQueue;
	//private final Object refreshQueueLock 						= new Object();
	
	private final Object refreshTriggerClientWaitLock 			= new Object();
	
	public ContextServiceClient(String hostName, int portNum) throws IOException
	{
		super(hostName, portNum);
		refreshTriggerQueue = new LinkedList<JSONObject>();
		sendConfigRequest();
	}
	
	@Override
	public void sendUpdate(String GUID, JSONObject gnsAttrValuePairs, long versionNum, boolean blocking)
	{
		ContextServiceLogger.getLogger().fine("ContextClient sendUpdate enter "+GUID+" json "+
				gnsAttrValuePairs);
		try
		{
			long currId;
			
			synchronized(this.updateIdLock)
			{
				currId = this.updateReqId++;
			}
			
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
			// no context service attribute matching.
			if(csAttrValuePairs.length() <= 0)
			{
				return;
			}
			
			long reqeustID = currId;
			
			ValueUpdateFromGNS<NodeIDType> valUpdFromGNS = 
					new ValueUpdateFromGNS<NodeIDType>(null, versionNum, GUID, csAttrValuePairs, reqeustID, sourceIP, sourcePort, 
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
			
			if(blocking)
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
		} catch (UnknownHostException e)
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
		if(replyArray == null)
		{
			ContextServiceLogger.getLogger().warning("null passsed "
					+ "as replyArray in sendSearchQuery");
			return -1;
		}
		
		long currId;
		synchronized(this.searchIdLock)
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
	
	public void handleUpdateReply(JSONObject jso)
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
			
			JSONArray nodeIPArray = configReply.getNodeConfigArray();
			JSONArray attrInfoArray = configReply.getAttributeArray();
			
			for(int i=0;i<nodeIPArray.length();i++)
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
			
			synchronized(this.configLock)
			{
				configLock.notify();
			}
		} catch (JSONException e)
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
			while(csNodeAddresses.size() == 0 )
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
			
			while(!refreshTriggerQueue.isEmpty())
			{
				triggerArray.put(refreshTriggerQueue.poll());
			}
		}
	}
}