package edu.umass.cs.contextservice.messages;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Class defines the packet type of the GNS trigger
 * @author ayadav
 */

public class ValueUpdateFromGNS<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {VERSION_NUM, GUID, ATTR_VALUE_PAIR, USER_REQUESTID, 
		SOURCEIP, SOURCEPORT, UPDATE_START_TIME, ANONYMIZEDID_TO_GUID_MAPPING};
	
	private final long versionNum;
	private final String GUID;
	private final JSONObject attrValuePair;
	private final long userRequestID;
	private final String sourceIP;
	private final int sourcePort;
	private final long updStartTime;
	private final JSONArray anonymizedIDToGuidMapping;
	
	public ValueUpdateFromGNS( NodeIDType initiator, long versionNum, String GUID, 
			JSONObject attrValuePair, long userRequestID, String sourceIP, int sourcePort, 
			long updStartTime, JSONArray anonymizedIDToGuidMapping )
	{
		super(initiator, ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS);
		this.versionNum = versionNum;
		this.GUID = GUID;
		this.attrValuePair = attrValuePair;
		this.userRequestID = userRequestID;
		this.sourceIP = sourceIP;
		this.sourcePort = sourcePort;
		this.updStartTime = updStartTime;
		this.anonymizedIDToGuidMapping = anonymizedIDToGuidMapping;
	}
	
	public ValueUpdateFromGNS(JSONObject json) throws JSONException
	{
		super(json);
		this.versionNum = json.getLong(Keys.VERSION_NUM.toString());
		this.GUID = json.getString(Keys.GUID.toString());
		this.attrValuePair = json.getJSONObject(Keys.ATTR_VALUE_PAIR.toString());
		this.userRequestID = json.getLong(Keys.USER_REQUESTID.toString());
		this.sourceIP = json.getString(Keys.SOURCEIP.toString());
		this.sourcePort = json.getInt(Keys.SOURCEPORT.toString());
		this.updStartTime = json.getLong(Keys.UPDATE_START_TIME.toString());
		
		if( json.has(Keys.ANONYMIZEDID_TO_GUID_MAPPING.toString()) )
		{
			this.anonymizedIDToGuidMapping 
					= json.getJSONArray(Keys.ANONYMIZEDID_TO_GUID_MAPPING.toString());
		}
		else
		{
			this.anonymizedIDToGuidMapping = null;
		}
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.VERSION_NUM.toString(), this.versionNum);
		json.put(Keys.GUID.toString(), this.GUID);
		json.put(Keys.ATTR_VALUE_PAIR.toString(), attrValuePair);
		json.put(Keys.USER_REQUESTID.toString(), this.userRequestID);
		json.put(Keys.SOURCEIP.toString(), this.sourceIP);
		json.put(Keys.SOURCEPORT.toString(), this.sourcePort);
		json.put(Keys.UPDATE_START_TIME.toString(), this.updStartTime);
		if(this.anonymizedIDToGuidMapping != null)
		{
			json.put(Keys.ANONYMIZEDID_TO_GUID_MAPPING.toString(), this.anonymizedIDToGuidMapping);
		}
		return json;
	}
	
	public long getVersionNum()
	{
		return this.versionNum;
	}
	
	public String getGUID()
	{
		return GUID;
	}
	
	public JSONObject getAttrValuePairs()
	{
		return this.attrValuePair;
	}
	
	public long getUserRequestID()
	{
		return this.userRequestID;
	}
	
	public String getSourceIP()
	{
		return this.sourceIP;
	}
	
	public int getSourcePort()
	{
		return this.sourcePort;
	}
	
	public long getUpdateStartTime()
	{
		return this.updStartTime;
	}
	
	public JSONArray getAnonymizedIDToGuidMapping()
	{
		return this.anonymizedIDToGuidMapping;
	}
	
	public static void main( String[] args )
	{
	}
}