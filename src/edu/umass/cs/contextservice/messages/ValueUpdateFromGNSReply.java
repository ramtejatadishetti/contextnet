package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class ValueUpdateFromGNSReply<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	// start time is the time when update started,
	// context time is the time at which context service recvd query
	// send time is the time when context service sends the ValueUpdateFromGNSReply
	private enum Keys {VERSION_NUM, START_TIME, CONTEXT_TIME, SEND_TIME};
	
	private final long versionNum;
	
	// just for debugging purposes
	private final long startTime;
	private final long contextTime;
	private final long sendTime;
	
	public ValueUpdateFromGNSReply(NodeIDType initiator, long versionNum, long startTime, long contextTime)
	{
		super(initiator, ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY);
		this.versionNum = versionNum;
		this.startTime = startTime;
		this.contextTime = contextTime;
		this.sendTime = System.currentTimeMillis();
	}
	
	public ValueUpdateFromGNSReply(JSONObject json) throws JSONException
	{
		//ValueUpdateFromGNS((NodeIDType)0, json.getString(Keys.GUID.toString()), 
		//		json.getDouble(Keys.OLDVALUE.toString()), json.getDouble(Keys.NEWVALUE.toString()));
		super(json);
		this.versionNum = json.getLong(Keys.VERSION_NUM.toString());
		this.startTime = json.getLong(Keys.START_TIME.toString());
		this.contextTime = json.getLong(Keys.CONTEXT_TIME.toString());
		this.sendTime = json.getLong(Keys.SEND_TIME.toString());
		//System.out.println("\n\n ValueUpdateFromGNS constructor");
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.VERSION_NUM.toString(), this.versionNum);
		json.put(Keys.SEND_TIME.toString(), this.sendTime);
		json.put(Keys.START_TIME.toString(), this.startTime);
		json.put(Keys.CONTEXT_TIME.toString(), this.contextTime);
		return json;
	}
	
	public long getVersionNum()
	{
		return this.versionNum;
	}
	
	public long getSendTime()
	{
		return this.sendTime;
	}
	
	public long getStartTime()
	{
		return this.startTime;
	}
	
	public long getContextTime()
	{
		return this.contextTime;
	}
	
	public static void main(String[] args)
	{
		/*int[] group = {3, 45, 6, 19};
		MetadataMsgToValuenode<Integer> se = 
		new MetadataMsgToValuenode<Integer>(4, "name1", 2, Util.arrayToIntSet(group), Util.arrayToIntSet(group));
		try
		{
			System.out.println(se);
			MetadataMsgToValuenode<Integer> se2 = new MetadataMsgToValuenode<Integer>(se.toJSONObject());
			System.out.println(se2);
			assert(se.toString().length()==se2.toString().length());
			assert(se.toString().indexOf("}") == se2.toString().indexOf("}"));
			assert(se.toString().equals(se2.toString())) : se.toString() + "!=" + se2.toString();
		} catch(JSONException je)
		{
			je.printStackTrace();
		}*/
	}
}