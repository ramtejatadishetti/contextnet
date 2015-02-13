package edu.umass.cs.contextservice.database;

import java.util.List;

import org.json.JSONObject;

import edu.umass.cs.contextservice.database.records.AttributeMetaObjectRecord;
import edu.umass.cs.contextservice.database.records.AttributeMetadataInfoRecord;
import edu.umass.cs.contextservice.database.records.ValueInfoObjectRecord;

/**
 * 
 * @author adipc
 */
public abstract class AbstractContextServiceDB<NodeIDType>
{
	protected final NodeIDType myID;
	//protected final InterfaceReconfigurableNodeConfig<NodeIDType> nodeConfig;
	//protected final ConsistentHashing<NodeIDType> CH_RC; // need to refresh when nodeConfig changes
	//protected final ConsistentHashing<NodeIDType> CH_AR; // need to refresh when nodeConfig changes
	
	/*public ReconfigurationRecord<NodeIDType> getReconfigurationRecord(String name, int epoch)
	{
		ReconfigurationRecord<NodeIDType> record = this.getReconfigurationRecord(name);
		return record.getEpoch()==epoch ? record : null;
	}*/
	
	// get methods
	/**
	 * Takes a JSONObject as input, that contains the query attributes 
	 * and returns the list of records.
	 * 
	 * @param queryAttrs
	 * @return
	 */
	public abstract AttributeMetadataInfoRecord<NodeIDType, Double> getAttributeMetaInfoRecord(String attrName);
	
	public abstract List<AttributeMetaObjectRecord<NodeIDType, Double>> 
						getAttributeMetaObjectRecord(String attrName, double queryMin, double queryMax);
	
	//public abstract AttributeValueInfoRecord getAttributeValueInfoRecord(String attrName);
	
	//public abstract GroupGUIDRecord getGroupGUIDRecord(String groupGUID);
	
	//public abstract List<NodeGUIDInfoRecord<Double>> getNodeGUIDInfoRecord
	//								(String attrName, double queryMin, double queryMax);
	
	//public abstract NodeGUIDInfoRecord<Double> getNodeGUIDInfoRecord(String attrName, String nodeGUID);
	
	public abstract List<ValueInfoObjectRecord<Double>> getValueInfoObjectRecord
												(String attrName, double queryMin, double queryMax);
	
	
	// put methods
	public abstract void putAttributeMetaInfoRecord(AttributeMetadataInfoRecord<NodeIDType, Double> putRec);
	
	public abstract void putAttributeMetaObjectRecord
							(AttributeMetaObjectRecord<NodeIDType, Double> putRec, String attrName);
	
	//public abstract void putAttributeValueInfoRecord(AttributeValueInfoRecord putRec);
	
	//public abstract void putGroupGUIDRecord(GroupGUIDRecord putRec);
	
	//public abstract void putNodeGUIDInfoRecord(String attrName, NodeGUIDInfoRecord<?> putRec);
	
	public abstract void putValueObjectRecord(ValueInfoObjectRecord<Double> putRec, String attrName);
	
	
	// update methods
	// having whole record in input
	public abstract void updateAttributeMetaObjectRecord
	(AttributeMetaObjectRecord<NodeIDType, Double> putRec, String attrName, JSONObject updateValue, 
			AttributeMetaObjectRecord.Operations operType, AttributeMetaObjectRecord.Keys fieldType);
	
	
	// updates valueInfoObjectRecord
	public abstract void updateValueInfoObjectRecord
	(ValueInfoObjectRecord<Double> putRec, String attrName, JSONObject updateValue, 
			ValueInfoObjectRecord.Operations operType, ValueInfoObjectRecord.Keys fieldType);
	
	// prints the database
	public abstract void printDatabase();
	
	// prints the database size
	public abstract long getDatabaseSize();
	
	// update methods
	// having whole record in input
	//public abstract void updateNodeGUIDInfoRecord
	//	(NodeGUIDInfoRecord<Double> putRec, String attrName, Object updateValue, 
	//			NodeGUIDInfoRecord.Operations operType, NodeGUIDInfoRecord.Keys fieldType);
	
	// having keys as input
	//public abstract void updateAttributeMetaObjectRecord
	//(Double rangeStart, Double rangeEnd, String attrName, Object updateValue, 
	//		AttributeMetaObjectRecord.Operations operType, AttributeMetaObjectRecord.Keys fieldType);
	
	
	//public abstract boolean updateStats(DemandReport<NodeIDType> report);
	//public abstract boolean setState(String name, int epoch, ReconfigurationRecord.RCStates state);
	//public abstract boolean setStateIfReady(String name, int epoch, ReconfigurationRecord.RCStates state);
	
	/*public boolean setStateSuper(String name, int epoch, ReconfigurationRecord.RCStates state)
	{
		ReconfigurationRecord<NodeIDType> record = this.getReconfigurationRecord(name);
		if(record==null || (state==ReconfigurationRecord.RCStates.WAIT_ACK_START && !record.incrEpoch(epoch-1)))  return false;
		record.setState(state);
		return true;
	}*/
	
	public AbstractContextServiceDB(NodeIDType myID)
	{
		this.myID = myID;
	}
	
	/*protected ReconfigurationRecord<NodeIDType> createRecord(String name)
	{
		ReconfigurationRecord<NodeIDType> record = null;
		try
		{
			record = new ReconfigurationRecord<NodeIDType>(name, 0, getInitialActives(name));
		} catch(JSONException je)
		{
			je.printStackTrace();
		}
		return record;
	}*/
	
	/*protected boolean amIResponsible(String name) 
	{
		Set<NodeIDType> nodes = CH_RC.getReplicatedServers(name);
		return (nodes.contains(myID) ? true : false); 
	}
	
	protected boolean amIFirst(String name) 
	{
		Set<NodeIDType> nodes = CH_RC.getReplicatedServers(name);
		return (nodes.contains(myID) && (nodes.iterator().next()==myID) ? true : false);
	}
	
	protected Set<NodeIDType> getDefaultActiveReplicas(String name)
	{
		return this.CH_RC.getReplicatedServers(name);
	}*/
	
	
	/* If the set of active replicas and reconfigurators is the same,
	 * this default policy will choose the reconfigurator nodes
	 * also as the initial set of active replicas.
	 */
	/*private Set<NodeIDType> getInitialActives(String name)
	{
		return CH_AR.getReplicatedServers(name);
	}*/

	/***************** Paxos related methods below ***********/
	/*
	@Override
	public boolean handleRequest(InterfaceRequest request,
			boolean doNotReplyToClient)
	{
		return false;
	}
	
	@Override
	public boolean handleRequest(InterfaceRequest request) 
	{
		return false;
	}
	
	@Override
	public InterfaceRequest getRequest(String stringified)
			throws RequestParseException 
	{
		return null;
	}
	
	@Override
	public Set<IntegerPacketType> getRequestTypes()
	{
		return null;
	}
	
	public String getState(String name, int epoch)
	{
		return null;
	}
	
	public boolean updateState(String name, String state)
	{
		return false;
	}*/
	
	public static void main(String[] args)
	{
		
	}
}