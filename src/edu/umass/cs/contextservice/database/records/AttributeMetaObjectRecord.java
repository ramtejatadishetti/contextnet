package edu.umass.cs.contextservice.database.records;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.database.MongoContextServiceDB;


/**
 * database record for AttributeMetadataObject class.
 * Keys for the record RANGE_START, RANGE_END pair
 * @author adipc
 *
 * @param <NodeIDType>
 * @param <AttributeIDType>
 */
public class AttributeMetaObjectRecord<NodeIDType, AttributeIDType> /*extends JSONObject*/
{
	public static enum Keys {RANGE_START, RANGE_END, NODE_ID, GROUP_GUID_LIST};
	
	public static enum Operations {APPEND, REPLACE, REMOVE};
	
	//public static enum RCStates {READY, WAIT_ACK_STOP, WAIT_ACK_START, WAIT_ACK_DROP};
	//private final String attributeName;
	private final Double rangeStart;
	private final Double rangeEnd;
	private final NodeIDType nodeID;
	private final JSONArray groupGUIDList;
	
	// primary key is in the form of JSON like { "$oid" : "546f092044ae941f7e5157a7"}
	private JSONObject primaryKeyJSON = null;
	
	public AttributeMetaObjectRecord(Double rangeStart, Double rangeEnd,
			NodeIDType nodeID, JSONArray groupGUIDList)
	{
		this.rangeStart = rangeStart;
		this.rangeEnd = rangeEnd;
		this.nodeID = nodeID;
		this.groupGUIDList = groupGUIDList;
	}
	
	public JSONObject toJSONObject() throws JSONException
	{
		JSONObject json = new JSONObject();
		json.put(Keys.RANGE_START.toString(), this.rangeStart);
		json.put(Keys.RANGE_END.toString(), this.rangeEnd);
		json.put(Keys.NODE_ID.toString(), this.nodeID);
		json.put(Keys.GROUP_GUID_LIST.toString(), this.groupGUIDList);
		return json;
	}
	
	public AttributeMetaObjectRecord(JSONObject json) throws JSONException
	{
		this.rangeStart = json.getDouble(Keys.RANGE_START.toString());
		this.rangeEnd = json.getDouble(Keys.RANGE_END.toString());
		this.nodeID = (NodeIDType) json.get(Keys.NODE_ID.toString());
		//this.groupGUIDList = json.getJSONArray(Keys.GROUP_GUID_LIST.toString());
		this.groupGUIDList = new JSONArray(json.getString(Keys.GROUP_GUID_LIST.toString()));
		
		try
		{
			this.primaryKeyJSON = json.getJSONObject(MongoContextServiceDB.PRIMARY_KEY);
			System.out.println("AttributeMetaObjectRecord primaryKeyJSON "+primaryKeyJSON);
		} catch(JSONException jso)
		{
			jso.printStackTrace();
		}
	}
	
	public String toString()
	{
		try
		{
			return this.toJSONObject().toString();
		}
		catch(JSONException je)
		{
			je.printStackTrace();
		}
		return null;
	}
	
	public double getRangeStart()
	{
		return this.rangeStart;
	}
	
	public double getRangeEnd()
	{
		return this.rangeEnd;
	}
	
	public NodeIDType getNodeID()
	{
		return this.nodeID;
	}
	
	public JSONArray getGroupGUIDList()
	{
		return this.groupGUIDList;
	}
	
	public JSONObject getPrimaryKeyJSON()
	{
		return this.primaryKeyJSON;
	}
	
	/*public ReconfigurationRecord<NodeIDType> putActiveReplicas(String name, int epoch, Set<NodeIDType> arSet)
	{
		if(epoch - this.epoch == 1)
		{
			this.newActives = arSet;
		} 
		return this;
	}
	
	public void setState(RCStates state)
	{
		this.state = state;
	}
	
	public RCStates getState() 
	{
		return this.state;
	}
	
	public String getName() 
	{
		return this.name;
	}*/
	
	/*@SuppressWarnings("unchecked")
	private Set<NodeIDType> toSet(JSONArray jsonArray) throws JSONException
	{
		Set<NodeIDType> set = new HashSet<NodeIDType>();
		for(int i=0; i<jsonArray.length(); i++)
		{
			set.add((NodeIDType)jsonArray.get(i));
		}
		return set;
	}*/
	
	public static void main(String[] args)
	{
		/*try
		{
			String name = "name1";
			int epoch = 23;
			Integer[] nodes = {2, 43, 54};
			Set<Integer> nodeSet =  new HashSet<Integer>(Arrays.asList(nodes));
			ReconfigurationRecord<Integer> rr1 = new ReconfigurationRecord<Integer>(name, epoch, nodeSet);
			rr1.putActiveReplicas(name, epoch+1, nodeSet);
			System.out.println(rr1.toJSONObject().toString());
			
			ReconfigurationRecord<Integer> rr2 = new ReconfigurationRecord<Integer>(rr1.toJSONObject());
			System.out.println(rr2.toString());
		} catch(JSONException je)
		{
			je.printStackTrace();
		}*/
	}
	
}