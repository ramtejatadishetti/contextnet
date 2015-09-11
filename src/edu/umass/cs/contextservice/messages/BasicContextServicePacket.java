package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;


/**
 * @author adipc
 *
 * @param <NodeIDType>
 */
public abstract class BasicContextServicePacket<NodeIDType> extends ContextServicePacket<NodeIDType>
{
	public BasicContextServicePacket(NodeIDType initiator, PacketType t) 
	{
		super(initiator);
		this.setType(t);	
		//this.serviceName = name;
		//this.epochNumber = epochNumber;
	}
	
	public BasicContextServicePacket(JSONObject json) throws JSONException 
	{
		super(json);
		
		//this.serviceName = json.getString(Keys.SERVICE_NAME.toString());
		//this.epochNumber = json.getInt(Keys.EPOCH_NUMBER.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException 
	{
		JSONObject json = super.toJSONObjectImpl();
		//json.put(Keys.SERVICE_NAME.toString(), this.serviceName);
		//json.put(Keys.EPOCH_NUMBER.toString(), this.epochNumber);
		return json;
	}
	
	/*public String getServiceName() 
	{
		return this.serviceName;
	}
	public int getEpochNumber() {
		return this.epochNumber;
	}*/
	

	public static void main(String[] args) 
	{
		/*class BRP extends BasicContextServicePacket<Integer> 
		{
			BRP(Integer initiator, PacketType t, String name, int epochNumber) {
				super(initiator, t, name, epochNumber);
			}
			BRP(JSONObject json) throws JSONException {
				super(json);
			}
		}
		BRP brc = new BRP(3, ContextServicePacket.PacketType.DEMAND_REPORT, "name1", 4);
		System.out.println(brc);
		try 
		{
			System.out.println(new BRP(brc.toJSONObject()));
		} catch(JSONException je) 
		{
			je.printStackTrace();
		}*/
	}
}