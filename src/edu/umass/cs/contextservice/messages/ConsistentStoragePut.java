package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class ConsistentStoragePut<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {GUID, ATTR_VALUE_PAIR, VERSION_NUM};
	
	//private final long reqID;
	private final String guid;
	private final JSONObject attrValuePair;
	private final long versionNum;
	//private final NodeIDType sourceID;
	
	/*
	 * sending more information, so that don't need to store request
	 */
	public ConsistentStoragePut(NodeIDType initiator, String guid, JSONObject attrValuePair, 
			long versionNum)
	{
		super(initiator, ContextServicePacket.PacketType.CONSISTENT_STORAGE_PUT);
		//this.reqID 			= reqID;
		this.guid 			= guid;
		this.attrValuePair 	= attrValuePair;
		this.versionNum 	= versionNum;
		//this.sourceID   	= sourceID; 
	}
	
	public ConsistentStoragePut(JSONObject json) throws JSONException
	{
		super(json);
		//this.reqID = json.getLong(Keys.REQ_ID.toString());
		this.guid = json.getString(Keys.GUID.toString());
		this.attrValuePair = json.getJSONObject(Keys.ATTR_VALUE_PAIR.toString());
		this.versionNum = json.getLong(Keys.VERSION_NUM.toString());
		//this.sourceID = (NodeIDType) ((Integer)json.getInt(Keys.SOURCEID.toString()));
		//this.query = json.getString(Keys.QUERY.toString());
		//this.sourceIP = json.getString(Keys.SOURCE_IP.toString());
		//this.sourcePort = json.getInt(Keys.SOURCE_PORT.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		//json.put(Keys.REQ_ID.toString(), reqID);
		json.put(Keys.GUID.toString(), guid);
		json.put(Keys.ATTR_VALUE_PAIR.toString(), attrValuePair);
		json.put(Keys.VERSION_NUM.toString(), this.versionNum);
		//json.put(Keys.SOURCEID.toString(), this.sourceID);
		//json.put(Keys.SOURCE_IP.toString(), sourceIP);
		//json.put(Keys.SOURCE_PORT.toString(), sourcePort);
		//json.put(Keys.USER_REQ_NUM.toString(), userReqNum);
		return json;
	}
	
	public String getGUID()
	{
		return this.guid;
	}
	
	public JSONObject getAttrValuePair()
	{
		return this.attrValuePair;
	}
	
	/*public long getRequestID()
	{
		return this.reqID;
	}*/
	
	public long getVersionNum()
	{
		return this.versionNum;
	}
	
	/*public NodeIDType getSourceID()
	{
		return this.sourceID;
	}*/
	
	public static void main(String[] args)
	{
	}
}