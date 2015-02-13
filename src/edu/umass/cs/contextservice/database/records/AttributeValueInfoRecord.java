package edu.umass.cs.contextservice.database.records;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.database.MongoContextServiceDB;

/**
 * Class to store the database record for AttributeValueInformation
 * Keys for the record ATTR_NAME 
 * FIXME: Initialize this table in attribute initialization.
 * @author adipc
 *
 */
public class AttributeValueInfoRecord
{
	
	public static enum Keys {ATTR_NAME, TABLE_NAME};
	//public static enum RCStates {READY, WAIT_ACK_STOP, WAIT_ACK_START, WAIT_ACK_DROP};
	//private final String attributeName;
	private final String attrName;
	private final String tableName;
	
	// primary key is in the form of JSON like { "$oid" : "546f092044ae941f7e5157a7"}
	private JSONObject primaryKeyJSON = null;
	
	public AttributeValueInfoRecord(String attrName, String tableName)
	{
		this.attrName = attrName;
		this.tableName = tableName;
	}
	
	public JSONObject toJSONObject() throws JSONException
	{
		JSONObject json = new JSONObject();
		json.put(Keys.ATTR_NAME.toString(), this.attrName);
		json.put(Keys.TABLE_NAME.toString(), this.tableName);
		return json;
	}
	
	public AttributeValueInfoRecord(JSONObject json) throws JSONException
	{
		this.attrName = json.getString(Keys.ATTR_NAME.toString());
		this.tableName = json.getString(Keys.TABLE_NAME.toString());
		
		try
		{
			this.primaryKeyJSON = json.getJSONObject(MongoContextServiceDB.PRIMARY_KEY);
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
	
	public String getAttributeName()
	{
		return this.attrName;
	}
	
	public String getTableName()
	{
		return this.tableName;
	}
	
	/*public NodeIDType getNodeID()
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
			System.out.println(rr1.toJSONObject().toString());
			
			ReconfigurationRecord<Integer> rr2 = new ReconfigurationRecord<Integer>(rr1.toJSONObject());
			System.out.println(rr2.toString());
		} catch(JSONException je)
		{
			je.printStackTrace();
		}*/
	}
}