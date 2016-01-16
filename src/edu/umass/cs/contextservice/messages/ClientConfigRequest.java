package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;


/**
 * message used by the client to request context service node ip address
 * and attributes supported
 * @author adipc
 *
 * @param <NodeIDType>
 */
public class ClientConfigRequest<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{	
	private enum Keys {SOURCEIP, SOURCEPORT};
	
	private final String sourceIP;
	private final int sourcePort;
	
	public ClientConfigRequest(NodeIDType initiator, String sourceIP, int sourcePort)
	{
		super(initiator, ContextServicePacket.PacketType.CONFIG_REQUEST);
		this.sourceIP = sourceIP;
		this.sourcePort = sourcePort;
	}
	
	public ClientConfigRequest(JSONObject json) throws JSONException
	{
		super(json);
		this.sourceIP   = json.getString(Keys.SOURCEIP.toString());
		this.sourcePort = json.getInt(Keys.SOURCEPORT.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.SOURCEIP.toString(), this.sourceIP);
		json.put(Keys.SOURCEPORT.toString(), this.sourcePort);
		return json;
	}
	
	public String getSourceIP()
	{
		return this.sourceIP;
	}
	
	public int getSourcePort()
	{
		return this.sourcePort;
	}
	
	public static void main(String[] args)
	{
	}
}