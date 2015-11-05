package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * used to fetch old value of the atrribute and 
 * @author adipc
 * @param <NodeIDType>
 */
public class ConsistentStoragePutReply<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {REQUEST_ID, GUID, ATTRNAME, OLD_VALUE, NEW_VALUE, VERSION_NUM, SOURCEID};
	
	private final long requestID;
	private final String guid;
	private final String attrName;
	private final double oldValue;
	private final double newValue;
	private final long versionNum;
	private final NodeIDType sourceID;
	
	public ConsistentStoragePutReply(NodeIDType initiator, long requestID, String guid, 
			String attrName, double oldValue, double newValue, long versionNum, NodeIDType sourceID)
	{
		super(initiator, ContextServicePacket.PacketType.CONSISTENT_STORAGE_PUT_REPLY);
		this.requestID = requestID;
		this.guid = guid;
		this.attrName = attrName;
		this.oldValue = oldValue;
		this.newValue = newValue;
		this.versionNum = versionNum;
		this.sourceID = sourceID;
	}
	
	public ConsistentStoragePutReply(JSONObject json) throws JSONException
	{
		super(json);
		this.requestID = json.getLong(Keys.REQUEST_ID.toString());
		this.guid = json.getString(Keys.GUID.toString());
		this.attrName = json.getString(Keys.ATTRNAME.toString());
		this.oldValue = json.getDouble(Keys.OLD_VALUE.toString());
		this.newValue = json.getDouble(Keys.NEW_VALUE.toString());
		this.versionNum = json.getLong(Keys.VERSION_NUM.toString());
		this.sourceID = (NodeIDType)((Integer)json.getInt(Keys.SOURCEID.toString()));		
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.REQUEST_ID.toString(), requestID);
		json.put(Keys.GUID.toString(), guid);
		json.put(Keys.ATTRNAME.toString(), attrName);
		json.put(Keys.OLD_VALUE.toString(), oldValue);
		json.put(Keys.NEW_VALUE.toString(), newValue);
		json.put(Keys.VERSION_NUM.toString(), versionNum);
		json.put(Keys.SOURCEID.toString(), sourceID);
	
		return json;
	}
	
	public long getRequestID()
	{
		return this.requestID;
	}
	
	public String getGUID()
	{
		return this.guid;
	}
	
	public String getAttrName()
	{
		return this.attrName;
	}
	
	public double getOldValue()
	{
		return this.oldValue;
	}
	
	public double getNewValue()
	{
		return this.newValue;
	}
	
	public long getVersionNum()
	{
		return this.versionNum;
	}
	
	public NodeIDType getSourceId()
	{
		return this.sourceID;
	}
	
	public static void main(String[] args)
	{
	}
}