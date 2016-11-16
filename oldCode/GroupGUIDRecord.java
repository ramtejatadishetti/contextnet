package edu.umass.cs.contextservice.database.records;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * database record for GroupGUIDInfo class.
 * Keys for the record GROUP_GUID
 * @author adipc
 *
 */
public class GroupGUIDRecord
{
	public static enum Keys {GROUP_GUID, GROUP_QUERY};
	//public static enum RCStates {READY, WAIT_ACK_STOP, WAIT_ACK_START, WAIT_ACK_DROP};
	//private final String attributeName;
	private final String groupGUID;
	private final String groupQuery;
	
	// primary key is in the form of JSON like { "$oid" : "546f092044ae941f7e5157a7"}
	//private JSONObject primaryKeyJSON = null;
	
	public GroupGUIDRecord(String groupGUID, String groupQuery)
	{
		this.groupGUID = groupGUID;
		this.groupQuery = groupQuery;
	}
	
	public JSONObject toJSONObject() throws JSONException
	{
		JSONObject json = new JSONObject();
		json.put(Keys.GROUP_GUID.toString(), this.groupGUID);
		json.put(Keys.GROUP_QUERY.toString(), this.groupQuery);
		return json;
	}
	
	public GroupGUIDRecord(JSONObject json) throws JSONException
	{
		this.groupGUID = json.getString(Keys.GROUP_GUID.toString());
		this.groupQuery = json.getString(Keys.GROUP_QUERY.toString());
		
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
	
	public String getGroupGUID()
	{
		return this.groupGUID;
	}
	
	public String getGroupQuery()
	{
		return this.groupQuery;
	}
	
	/*public Integer getNodeID()
	{
		return this.nodeID;
	}
	
	public JSONArray getGroupGUIDList()
	{
		return this.groupGUIDList;
	}*/
	
	/*public ReconfigurationRecord<Integer> putActiveReplicas(String name, int epoch, Set<Integer> arSet)
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
	private Set<Integer> toSet(JSONArray jsonArray) throws JSONException
	{
		Set<Integer> set = new HashSet<Integer>();
		for(int i=0; i<jsonArray.length(); i++)
		{
			set.add((Integer)jsonArray.get(i));
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