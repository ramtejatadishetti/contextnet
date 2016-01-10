package edu.umass.cs.contextservice.database.records;

import org.json.JSONException;
import org.json.JSONObject;



/**
 * Class that represents the record in database.
 * Database record for AttributeMetadataInformation class.
 * Keys for the record ATTR_NAME
 * FIXME: Initialize this table in attribute initialization.
 * @author adipc
 *
 */
public class AttributeMetadataInfoRecord<NodeIDType, AttributeIDType> /*extends JSONObject*/
{
	public static enum Keys {ATTR_NAME, ATTR_MIN, ATTR_MAX, TABLE_NAME};
	//public static enum RCStates {READY, WAIT_ACK_STOP, WAIT_ACK_START, WAIT_ACK_DROP};
	private final String attributeName;
	private final double attrMin;
	private final double attrMax;
	private final String tableName;
	// primary key is in the form of JSON like { "$oid" : "546f092044ae941f7e5157a7"}
	private JSONObject primaryKeyJSON = null;
	
	public AttributeMetadataInfoRecord(String attributeName, double attrMin,
			double attrMax)
	{
		this.attributeName = attributeName;
		this.attrMin = attrMin;
		this.attrMax = attrMax;
		// table name concatenation of two.
		//this.tableName = MongoContextServiceDB.ATTR_META_OBJ_TABLE_PREFIX+attributeName;
		this.tableName = null;
	}
	
	public JSONObject toJSONObject() throws JSONException
	{
		JSONObject json = new JSONObject();
		json.put(Keys.ATTR_NAME.toString(), this.attributeName);
		json.put(Keys.ATTR_MIN.toString(), this.attrMin);
		json.put(Keys.ATTR_MAX.toString(), this.attrMax);
		json.put(Keys.TABLE_NAME.toString(), this.tableName);
		return json;
	}
	
	
	/**
	 * JSON here comes from database, so we can intialize primary key
	 * @param json
	 * @throws JSONException
	 */
	public AttributeMetadataInfoRecord(JSONObject json) throws JSONException
	{
		this.attributeName = json.getString(Keys.ATTR_NAME.toString());
		this.attrMin = json.getDouble(Keys.ATTR_MIN.toString());
		this.attrMax = json.getDouble(Keys.ATTR_MAX.toString());
		this.tableName = json.getString(Keys.TABLE_NAME.toString());
		
		try
		{
			this.primaryKeyJSON = json.getJSONObject(MongoContextServiceDB.PRIMARY_KEY);
		} catch(JSONException jso)
		{
			jso.printStackTrace();
		}
	}
	
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
	
	public String getAttrName()
	{
		return this.attributeName;
	}
	
	public double getAttrMin()
	{
		return this.attrMin;
	}
	
	public double getAttrMax()
	{
		return this.attrMax;
	}
	
	public String getTableName()
	{
		return this.tableName;
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