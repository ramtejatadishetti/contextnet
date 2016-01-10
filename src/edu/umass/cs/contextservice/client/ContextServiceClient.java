package edu.umass.cs.contextservice.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.client.storage.GetStorage;
import edu.umass.cs.contextservice.client.storage.SearchQueryStorage;
import edu.umass.cs.contextservice.client.storage.UpdateStorage;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.GetMessage;
import edu.umass.cs.contextservice.messages.GetReplyMessage;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;
import edu.umass.cs.contextservice.messages.RefreshTrigger;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNSReply;

public class ContextServiceClient<NodeIDType> extends AbstractContextServiceClient<NodeIDType>
{	
	public ContextServiceClient() throws IOException
	{
		super();
	}
	
	@Override
	public void sendUpdate(String GUID, JSONObject gnsAttrValuePairs, long versionNum) 
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
				//ContextServiceLogger.getLogger().fine("ContextClient gnsAttrName "+gnsAttrName+" currId "+
				//		currId);
				// this attribute is context attribute,
				// indexed in context service
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
			
			//ContextServiceLogger.getLogger().fine("ContextClient sendUpdate enter "+GUID+" processed json "+
			//		csAttrValuePairs);
			long reqeustID = currId;
			
			//JSONObject valUpdateJSON = createValueUpdateJSON(versionNum, GUID, 
			//		csAttrValuePairs, sourceIP, sourcePort, reqeustID);
			//System.out.println("ValueUpdateFromGNS this.nodeid "+this.nodeid);
			
			ValueUpdateFromGNS<NodeIDType> valUpdFromGNS = 
					new ValueUpdateFromGNS<NodeIDType>(null, versionNum, GUID, csAttrValuePairs, sourceIP, sourcePort, reqeustID );
			
			//System.out.println("ValueUpdateFromGNS after");
			//ContextServiceLogger.getLogger().fine("ContextClient sendUpdate valUpdFromGNS "+valUpdFromGNS);
			
			UpdateStorage<NodeIDType> updateQ = new UpdateStorage<NodeIDType>();
			updateQ.requestID = currId;
			//updateQ.valUpdFromGNS = valUpdFromGNS;
			updateQ.valUpdFromGNS = valUpdFromGNS;
			updateQ.valUpdFromGNSReply = null;
			
			this.pendingUpdate.put(currId, updateQ);
			
			InetSocketAddress sockAddr = this.csNodeAddresses.get(rand.nextInt(csNodeAddresses.size()));
			
			ContextServiceLogger.getLogger().fine("ContextClient sending update to "+sockAddr+" json "+
					valUpdFromGNS);		
			//niot.sendToAddress(sockAddr, valUpdFromGNS.toJSONObject());
			niot.sendToAddress(sockAddr, valUpdFromGNS.toJSONObject());
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
	public JSONArray sendSearchQuery(String searchQuery) 
	{
		//JSONObject geoJSONObject = getGeoJSON();
		//String query = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE GeojsonOverlap("+geoJSONObject.toString()+")";
		//eservice.execute(new SendingRequest(currID, SendingRequest.QUERY, query, currNumAttr, "", -1, -1, "") );
		//currNumAttr = currNumAttr + 2;
		
		long currId;
		synchronized(this.searchIdLock)
		{
			currId = this.searchReqId++;
		}
		
		QueryMsgFromUser<NodeIDType> qmesgU 
			= new QueryMsgFromUser<NodeIDType>(this.nodeid, searchQuery, sourceIP, sourcePort, currId);
		
		SearchQueryStorage<NodeIDType> searchQ = new SearchQueryStorage<NodeIDType>();
		searchQ.requestID = currId;
		searchQ.queryMsgFromUser = qmesgU;
		searchQ.queryMsgFromUserReply = null; 
		
		this.pendingSearches.put(currId, searchQ);
		InetSocketAddress sockAddr = this.csNodeAddresses.get(rand.nextInt(csNodeAddresses.size()));
		//ContextServiceLogger.getLogger().fine("Sending query to "+sockAddr);
		
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
		pendingSearches.remove(currId);
		
		// convert result to list of GUIDs, currently is is list of JSONArrays 
		JSONArray resultRet = new JSONArray();
		for(int i=0; i<result.length(); i++)
		{
			try
			{
				JSONArray jsoArr1 = result.getJSONArray(i);
				for(int j=0; j<jsoArr1.length(); j++)
				{
					resultRet.put(jsoArr1.getString(j));
				}
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
		
		return resultRet;
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
				//handleRefreshTrigger(jsonObject);
			} else if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE)
					== ContextServicePacket.PacketType.GET_REPLY_MESSAGE.getInt() )
			{
				handleGetReply(jsonObject);
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
		this.pendingUpdate.remove(currReqID);
		/*long currReqID;
		try {
			currReqID = jso.getLong(ValueUpdateReplyKeys.USER_REQ_NUM.toString());
			this.pendingUpdate.remove(currReqID);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		
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
	
	private void handleRefreshTrigger(JSONObject jso)
	{
		try
		{
			RefreshTrigger<NodeIDType> qmur;
			qmur = new RefreshTrigger<NodeIDType>(jso);
			
			long reqID = qmur.getVersionNum();
			
		
			System.out.println("RefreshTrigger completion requestID "+reqID+" time "+System.currentTimeMillis());
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
}