package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class ValueUpdateFromGNSReply<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {VERSION_NUM};
	
	private final long versionNum;
	
	public ValueUpdateFromGNSReply(NodeIDType initiator, long versionNum)
	{
		super(initiator, ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY);
		this.versionNum = versionNum;
	}
	
	public ValueUpdateFromGNSReply(JSONObject json) throws JSONException
	{
		//ValueUpdateFromGNS((NodeIDType)0, json.getString(Keys.GUID.toString()), 
		//		json.getDouble(Keys.OLDVALUE.toString()), json.getDouble(Keys.NEWVALUE.toString()));
		super(json);
		this.versionNum = json.getLong(Keys.VERSION_NUM.toString());
		//System.out.println("\n\n ValueUpdateFromGNS constructor");
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.VERSION_NUM.toString(), this.versionNum);
		return json;
	}
	
	public long getVersionNum()
	{
		return this.versionNum;
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