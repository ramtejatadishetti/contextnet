package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.processing.QueryComponent;

/**
 * Message is sent from metadata node to the valuenode, 
 * for each query
 * @author adipc
 *
 * @param <NodeIDType>
 */
public class QueryMsgToValuenode<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	/**
	 * PREDICATE: predicate for this message, like 1 <= contextATT0 <= 1000 is a valid predicate.
	 * SOURCE_ID: is the id of the query source, reply from value nodes goes directly to the sourceid.
	 * REQUESTID: id unique to the request, need for demultiplexing at the source.
	 * qUERY: String query in mint condition from the user 
	 * GROUP_GUID: GroupGUID
	 * NUM_VAL_NODES_CONTACTED: denotes number of value nodes contacted for this predicate.
	 *                          It is set by the metadata node and is used by the query source to 
	 *                          find out whether all query replies have been received or not.
	 * @author adipc
	 */
	private enum Keys {PREDICATE, SOURCE_ID, REQUESTID, qUERY, GROUP_GUID, NUM_VAL_NODES_CONTACTED};
	
	private final QueryComponent predicate;
	private final NodeIDType sourceNodeId;
	private final long requestID;
	
	// additional info for trigger to update groups on
	// value updates
	// whole query
	private final String query;
	// GUID of group associated with this query
	private final String groupGUID;
	
	private final int numValueNodesContacted;
	
	
	public QueryMsgToValuenode(NodeIDType initiator, QueryComponent predicate, long requestId, NodeIDType sourceID,
			String query, String groupGUID, int numValNodesCon)
	{
		super(initiator, ContextServicePacket.PacketType.QUERY_MSG_TO_VALUENODE);
		this.predicate = predicate;
		this.sourceNodeId = sourceID;
		this.requestID = requestId;
		this.query = query;
		this.groupGUID = groupGUID;
		this.numValueNodesContacted = numValNodesCon;
	}
	
	@SuppressWarnings("unchecked")
	public QueryMsgToValuenode(JSONObject json) throws JSONException
	{
		super(json);
		this.predicate = QueryComponent.getQueryComponent(json.getJSONObject(Keys.PREDICATE.toString()));
		this.sourceNodeId = (NodeIDType) json.get(Keys.SOURCE_ID.toString());
		this.requestID = json.getLong(Keys.REQUESTID.toString());
		this.query = json.getString(Keys.qUERY.toString());
		this.groupGUID = json.getString(Keys.GROUP_GUID.toString());
		this.numValueNodesContacted = json.getInt(Keys.NUM_VAL_NODES_CONTACTED.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.PREDICATE.toString(), predicate.getJSONObject());
		json.put(Keys.SOURCE_ID.toString(), sourceNodeId);
		json.put(Keys.REQUESTID.toString(), requestID);
		json.put(Keys.qUERY.toString(), this.query);
		json.put(Keys.GROUP_GUID.toString(), this.groupGUID);
		json.put(Keys.NUM_VAL_NODES_CONTACTED.toString(), this.numValueNodesContacted);
		return json;
	}
	
	public QueryComponent getQueryComponent()
	{
		return predicate;
	}
	
	public long getRequestId()
	{
		return requestID;
	}
	
	public NodeIDType getSourceId()
	{
		return sourceNodeId;
	}
	
	public String getQuery()
	{
		return query;
	}
	
	public String getGropGUID()
	{
		return this.groupGUID;
	}
	
	public int getNumValNodesContacted()
	{
		return this.numValueNodesContacted;
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