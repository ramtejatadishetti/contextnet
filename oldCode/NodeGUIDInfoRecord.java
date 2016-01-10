package edu.umass.cs.contextservice.database.records;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * 
 * Class to store the database record for NodeGUIDInfoRecord.
 * Keys for the record NODE_GUID
 * FIXME: supports double for now, but should be made generic 
 * for many datatypes
 * @author adipc
 */
public class NodeGUIDInfoRecord<AttributeType>
{
	public static enum Keys {NODE_GUID, ATTR_VALUE, VERSION_NUM, FULL_OBJECT};
	
	public static enum Operations {APPEND, REPLACE, REMOVE};
	
	//public static enum RCStates {READY, WAIT_ACK_STOP, WAIT_ACK_START, WAIT_ACK_DROP};
	//private final String attributeName;
	private final String nodeGUID;
	private final double attrValue;
	private final long versionNum;
	// needed by other schemes
	// attributes are keys and values are attribute values
	private final JSONObject fullDataObject;
	
	// primary key is in the form of JSON like { "$oid" : "546f092044ae941f7e5157a7"}
	//private JSONObject primaryKeyJSON = null;
	
	public NodeGUIDInfoRecord(String nodeGUID, double attrValue, long versionNum, 
			JSONObject fullDataObject)
	{
		this.nodeGUID = nodeGUID;
		this.attrValue = attrValue;
		this.versionNum = versionNum;
		this.fullDataObject = fullDataObject;
	}
	
	public JSONObject toJSONObject() throws JSONException
	{
		JSONObject json = new JSONObject();
		json.put(Keys.NODE_GUID.toString(), this.nodeGUID);
		json.put(Keys.ATTR_VALUE.toString(), this.attrValue);
		json.put(Keys.VERSION_NUM.toString(), this.versionNum);
		json.put(Keys.FULL_OBJECT.toString(), this.fullDataObject);
		return json;
	}
	
	public NodeGUIDInfoRecord(JSONObject json) throws JSONException
	{
		this.nodeGUID = json.getString(Keys.NODE_GUID.toString());
		this.attrValue = json.getDouble(Keys.ATTR_VALUE.toString());
		this.versionNum = json.getLong(Keys.VERSION_NUM.toString());
		this.fullDataObject = json.getJSONObject(Keys.FULL_OBJECT.toString());
		
		/*try
		{
			this.primaryKeyJSON = json.getJSONObject(MongoContextServiceDB.PRIMARY_KEY);
		} catch(JSONException jso)
		{
			jso.printStackTrace();
		}*/
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
	
	public String getNodeGUID()
	{
		return this.nodeGUID;
	}
	
	public double getAttrValue()
	{
		return this.attrValue;
	}
	
	public long getVersionNum()
	{
		return this.versionNum;
	}
	
	public JSONObject getFullDataObject()
	{
		return this.fullDataObject;
	}
	
	/*public JSONObject getPrimaryKeyJSON()
	{
		return this.primaryKeyJSON;
	}
	
	public NodeIDType getNodeID()
	{
		return this.nodeID;
	}
	
	public JSONArray getGroupGUIDList()
	{
		return this.groupGUIDList;
	}*/
	
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
			ContextServiceLogger.getLogger().fine(rr1.toJSONObject().toString());
			
			ReconfigurationRecord<Integer> rr2 = new ReconfigurationRecord<Integer>(rr1.toJSONObject());
			ContextServiceLogger.getLogger().fine(rr2.toString());
		} catch(JSONException je)
		{
			je.printStackTrace();
		}*/
	}
	
}