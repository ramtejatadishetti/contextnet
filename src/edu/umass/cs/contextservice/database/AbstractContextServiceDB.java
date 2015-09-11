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
	
	
	public abstract List<ValueInfoObjectRecord<Double>> getValueInfoObjectRecord
												(String attrName, double queryMin, double queryMax);
	
	
	// put methods
	public abstract void putAttributeMetaInfoRecord(AttributeMetadataInfoRecord<NodeIDType, Double> putRec);
	
	public abstract void putAttributeMetaObjectRecord
							(AttributeMetaObjectRecord<NodeIDType, Double> putRec, String attrName);
	
	
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
	
	// update the GUID's attribute value
	public abstract boolean updateGUIDRecord(String GUID, String attrName, double value);
	
	// gets GUID record
	public abstract JSONObject getGUIDRecord(String GUID);
	
	public AbstractContextServiceDB(NodeIDType myID)
	{
		this.myID = myID;
	}
	
	
	// SQL schema methods
	//public abstract List<AttributeMetaObjectRecord<NodeIDType, Double>> 
	//				getAttributeMetaObjectRecord(String attrName, double queryMin, double queryMax);
	
	
	public static void main(String[] args)
	{
	}
}