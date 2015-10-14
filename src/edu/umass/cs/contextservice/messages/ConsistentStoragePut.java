package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class BulkPut<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {GUID, ATTRNAME, VALUE};
	
	private final String guid;
	private final String attrName;
	private final double value;
	
	public BulkPut(NodeIDType initiator, String guid, String attrName, double value)
	{
		super(initiator, ContextServicePacket.PacketType.BULK_PUT);
		this.guid = guid;
		this.attrName = attrName;
		this.value = value;
	}
	
	public BulkPut(JSONObject json) throws JSONException
	{
		super(json);
		this.guid = json.getString(Keys.GUID.toString());
		this.attrName = json.getString(Keys.ATTRNAME.toString());
		this.value = json.getDouble(Keys.VALUE.toString());
		//this.query = json.getString(Keys.QUERY.toString());
		//this.sourceIP = json.getString(Keys.SOURCE_IP.toString());
		//this.sourcePort = json.getInt(Keys.SOURCE_PORT.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.GUID.toString(), guid);
		json.put(Keys.ATTRNAME.toString(), attrName);
		json.put(Keys.VALUE.toString(), value);
		
		//json.put(Keys.SOURCE_IP.toString(), sourceIP);
		//json.put(Keys.SOURCE_PORT.toString(), sourcePort);
		//json.put(Keys.USER_REQ_NUM.toString(), userReqNum);
		return json;
	}
	
	public String getGUID()
	{
		return this.guid;
	}
	
	public String getAttrName()
	{
		return this.attrName;
	}
	
	public double getValue()
	{
		return this.value;
	}
	
	public static void main(String[] args)
	{
	}
}