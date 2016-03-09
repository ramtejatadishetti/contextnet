package edu.umass.cs.contextservice.messages;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class RefreshTrigger<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	// 1 for add, 2 for remove
	
	public static final int ADD						= 1;
	public static final int REMOVE					= 2;
	
	private enum Keys {GROUP_GUID, VERSION_NUM, GUID, ADD_REMOVE, UPDATE_START_TIME};
	
	//private final String query;  // original query sent by the user.
	private final JSONArray groupGUIDArr;
	private final long versionNum;
	private final String updateInGUID;
	private final int addRemove;
	// indicates the update start time for this trigger, the udpate that caused this trigger. 
	private final long updStartTime;
	
	public RefreshTrigger(NodeIDType initiator, JSONArray groupGUIDArr, long versionNum,
			String GUID, int addRemove, long updStartTime)
	{
		super(initiator, ContextServicePacket.PacketType.REFRESH_TRIGGER);
		
		this.groupGUIDArr = groupGUIDArr;
		//this.query = query;
		this.versionNum = versionNum;
		this.updateInGUID = GUID;
		this.addRemove = addRemove;
		this.updStartTime = updStartTime;
	}
	
	public RefreshTrigger(JSONObject json) throws JSONException
	{
		super(json);
		
		this.groupGUIDArr = json.getJSONArray(Keys.GROUP_GUID.toString());
		//this.query = json.getString(Keys.QUERY.toString());
		this.versionNum = json.getLong(Keys.VERSION_NUM.toString());
		this.updateInGUID = json.getString(Keys.GUID.toString());
		this.addRemove = json.getInt(Keys.ADD_REMOVE.toString());
		this.updStartTime = json.getLong(Keys.UPDATE_START_TIME.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.GROUP_GUID.toString(), groupGUIDArr);
		//json.put(Keys.QUERY.toString(), query);
		json.put(Keys.VERSION_NUM.toString(), versionNum);
		json.put(Keys.GUID.toString(), updateInGUID);
		json.put(Keys.ADD_REMOVE.toString(), addRemove);
		json.put(Keys.UPDATE_START_TIME.toString(), updStartTime);
		return json;
	}
	
	public JSONArray getGroupGUID()
	{
		return this.groupGUIDArr;
	}
	
	public long getVersionNum()
	{
		return this.versionNum;
	}
	
	public String getUpdateGUID()
	{
		return this.updateInGUID;
	}
	
	public int getAddRemove()
	{
		return this.addRemove;
	}
	
	public long getUpdateStartTime()
	{
		return this.updStartTime;
	}
	
	public static void main(String[] args)
	{
	}
	
//	public String getQuery()
//	{
//		return query;
//	}
}