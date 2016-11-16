package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author adipc
 *
 * @param <Integer>
 */
public class MetadataMsgToValuenode<Integer> extends BasicContextServicePacket<Integer>
{
	private enum Keys {ATTR_NAME, RANGE_START, RANGE_END};
	
	private final String attrName;
	private final double rangeStart;
	private final double rangeEnd;

	public MetadataMsgToValuenode(Integer initiator, String attrName, 
			double rangeStart, double rangeEnd) 
	{
		super(initiator, ContextServicePacket.PacketType.METADATA_MSG_TO_VALUENODE);
		this.attrName = attrName;
		this.rangeStart = rangeStart;
		this.rangeEnd = rangeEnd;
	}

	public MetadataMsgToValuenode(JSONObject json) throws JSONException 
	{
		super(json);
		this.attrName = json.getString(Keys.ATTR_NAME.toString());
		this.rangeStart = json.getDouble(Keys.RANGE_START.toString());
		this.rangeEnd = json.getDouble(Keys.RANGE_END.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException 
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.ATTR_NAME.toString(), attrName);
		json.put(Keys.RANGE_START.toString(), rangeStart);
		json.put(Keys.RANGE_END.toString(), rangeEnd);
		return json;
	}

	public String getAttrName()
	{
		return attrName;
	}
	
	public double getRangeStart() 
	{
		return rangeStart;
	}
	
	public double getRangeEnd() 
	{
		return rangeEnd;
	}
	
	public static void main(String[] args) 
	{
		/*int[] group = {3, 45, 6, 19};
		MetadataMsgToValuenode<Integer> se = 
				new MetadataMsgToValuenode<Integer>(4, "name1", 2, Util.arrayToIntSet(group), Util.arrayToIntSet(group));
		try 
		{
			ContextServiceLogger.getLogger().fine(se);
			MetadataMsgToValuenode<Integer> se2 = new MetadataMsgToValuenode<Integer>(se.toJSONObject());
			ContextServiceLogger.getLogger().fine(se2);
			assert(se.toString().length()==se2.toString().length());
			assert(se.toString().indexOf("}") == se2.toString().indexOf("}"));
			assert(se.toString().equals(se2.toString())) : se.toString() + "!=" + se2.toString();
		} catch(JSONException je)
		{
			je.printStackTrace();
		}*/
	}
}