package edu.umass.cs.contextservice.messages;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Reply of the value node, consisting of the list of GUIDs back to
 * the source of the query. 
 * @author adipc
 */
public class QueryMsgToValuenodeReply<Integer> extends BasicContextServicePacket<Integer>
{
	private enum Keys {GUIDs, SOURCE_ID, REQUESTID, COMPONENT_ID, NUM_VAL_NODES_CONTACTED};
	
	private final JSONArray resultGUIDs;
	private final Integer sourceNodeId;
	private final long requestID;    // queryID
	private final int componentID;   // component within the query ID
	
	
	// see QueryMsgToValuenode.java for details of this field.
	private final int numValNodesCont;
	
	
	public QueryMsgToValuenodeReply(Integer initiator, JSONArray resultGUIDs, 
			long requestID, int componentID, Integer sourceID, int numValNodesCont)
	{
		super(initiator, ContextServicePacket.PacketType.QUERY_MSG_TO_VALUENODE_REPLY);
		
		this.resultGUIDs = resultGUIDs;
		
		/*for(int i=0;i<resultGUIDs.size();i++)
		{
			try
			{
				this.resultGUIDs.put(i, resultGUIDs.get(i));
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}*/
		this.sourceNodeId = sourceID;
		this.requestID = requestID;
		this.componentID = componentID;
		this.numValNodesCont = numValNodesCont;
	}
	
	@SuppressWarnings("unchecked")
	public QueryMsgToValuenodeReply(JSONObject json) throws JSONException
	{
		super(json);
		
		this.resultGUIDs = json.getJSONArray(Keys.GUIDs.toString());
		
		this.sourceNodeId = (Integer)json.get(Keys.SOURCE_ID.toString());
		this.requestID = json.getLong(Keys.REQUESTID.toString());
		this.componentID = json.getInt(Keys.COMPONENT_ID.toString());
		this.numValNodesCont = json.getInt(Keys.NUM_VAL_NODES_CONTACTED.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.GUIDs.toString(), resultGUIDs);
		json.put(Keys.SOURCE_ID.toString(), sourceNodeId);
		json.put(Keys.REQUESTID.toString(), requestID);
		json.put(Keys.COMPONENT_ID.toString(), componentID);
		json.put(Keys.NUM_VAL_NODES_CONTACTED.toString(), this.numValNodesCont);
		return json;
	}
	
	public JSONArray getResultGUIDs()
	{
		return this.resultGUIDs;
	}
	
	public Integer getSourceID()
	{
		return this.sourceNodeId;
	}
	
	public long getRequestID()
	{
		return this.requestID;
	}
	
	public int getComponentID()
	{
		return this.componentID;
	}
	
	public int getNumValNodesContacted()
	{
		return this.numValNodesCont;
	}
	
	public static void main(String[] args)
	{
		
	}
}