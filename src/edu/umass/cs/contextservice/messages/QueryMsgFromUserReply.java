package edu.umass.cs.contextservice.messages;

import java.util.LinkedList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class QueryMsgFromUserReply<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {QUERY, QUERY_GUID, GUIDs, USER_REQ_NUM};
	
	private final String query;  // original query sent by the user.
	private final String queryGUID;
	private final JSONArray resultGUIDs;
	private final long userReqNum;
	
	public QueryMsgFromUserReply(NodeIDType initiator, String query, String queryGUID, JSONArray resultGUIDs
			, long userReqNum)
	{
		super(initiator, ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY);
		
		this.resultGUIDs = resultGUIDs;
		
		this.query = query;
		this.queryGUID = queryGUID;
		this.userReqNum = userReqNum;
	}
	
	public QueryMsgFromUserReply(JSONObject json) throws JSONException
	{
		super(json);
		
		this.resultGUIDs = json.getJSONArray(Keys.GUIDs.toString());
		this.query = json.getString(Keys.QUERY.toString());
		this.userReqNum = json.getLong(Keys.USER_REQ_NUM.toString());
		this.queryGUID = json.getString(Keys.QUERY_GUID.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.GUIDs.toString(), resultGUIDs);
		json.put(Keys.QUERY.toString(), query);
		json.put(Keys.USER_REQ_NUM.toString(), this.userReqNum);
		json.put(Keys.QUERY_GUID.toString(), this.queryGUID);
		return json;
	}
	
	public JSONArray getResultGUIDs()
	{
		return this.resultGUIDs;
	}
	
	public String getQuery()
	{
		return query;
	}
	
	public long getUserReqNum()
	{
		return this.userReqNum;
	}
	
	public String getQueryGUID()
	{
		return this.queryGUID;
	}
	
	public static void main(String[] args)
	{
		
	}
}