package edu.umass.cs.contextservice.database;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.records.AttributeMetaObjectRecord;
import edu.umass.cs.contextservice.database.records.AttributeMetaObjectRecord.Keys;
import edu.umass.cs.contextservice.database.records.AttributeMetaObjectRecord.Operations;
import edu.umass.cs.contextservice.database.records.AttributeMetadataInfoRecord;
import edu.umass.cs.contextservice.database.records.NodeGUIDInfoRecord;
import edu.umass.cs.contextservice.database.records.ValueInfoObjectRecord;

/**
 * maintains inmemory database for the context service
 * @author adipc
 * @param <NodeIDType>
 */
public class InMemoryContextServiceDB<NodeIDType> extends AbstractContextServiceDB<NodeIDType>
{
	private final HashMap<String, AttributeMetadataInfoRecord<NodeIDType, Double>>
																	attrMetaInfoTable;
	
	private final HashMap<String, LinkedList<AttributeMetaObjectRecord<NodeIDType, Double>>>
																	metaObjTable;
	
	private final HashMap<String, LinkedList<ValueInfoObjectRecord<Double>>>
																	valObjTable;
	
	private final Object updValInfoObjRecMonitor;
	
	
	public InMemoryContextServiceDB(NodeIDType myID)
	{
		super(myID);
		attrMetaInfoTable 	= new HashMap<String, AttributeMetadataInfoRecord<NodeIDType, Double>>();
		metaObjTable 		= new HashMap<String, LinkedList<AttributeMetaObjectRecord<NodeIDType, Double>>>();
		valObjTable 		= new HashMap<String, LinkedList<ValueInfoObjectRecord<Double>>>();
		
		// sync monintors
		updValInfoObjRecMonitor = new Object();
	}
	
	@Override
	public AttributeMetadataInfoRecord<NodeIDType, Double> getAttributeMetaInfoRecord(
			String attrName)
	{
		return attrMetaInfoTable.get(attrName);
	}
	
	@Override
	public List<AttributeMetaObjectRecord<NodeIDType, Double>> getAttributeMetaObjectRecord(
			String attrName, double queryMin, double queryMax)
	{
		LinkedList<AttributeMetaObjectRecord<NodeIDType, Double>> attrMetaObjList = 
				metaObjTable.get(attrName);
		
		LinkedList<AttributeMetaObjectRecord<NodeIDType, Double>> resultList = 
				new LinkedList<AttributeMetaObjectRecord<NodeIDType, Double>>();
		
		for(int i=0;i<attrMetaObjList.size();i++)
		{
			AttributeMetaObjectRecord<NodeIDType, Double> attrMetaObjRec = 
					attrMetaObjList.get(i);
			
			// trying to find if there is an overlap in the ranges, 
			// the range specified by user and the range in database.
			// overlap is there if queryMin lies between the range in database
			// or queryMax lies between the range in database.
			// So, we specify two or conditions.
			
			boolean queryMinInBetween = (queryMin >= attrMetaObjRec.getRangeStart()) 
					&& (queryMin < attrMetaObjRec.getRangeEnd());
			
			// for right side value, it can't be equal to rangestart, 
			// but it can be equal to rangeEnd, although even then it doesn't include
			// rangeEnd.
			boolean queryMaxInBetween = (queryMax > attrMetaObjRec.getRangeStart()) 
					&& (queryMax <= attrMetaObjRec.getRangeEnd());
			
			// or on the two conditions
			if(queryMinInBetween || queryMaxInBetween)
			{
				resultList.add(attrMetaObjRec);
			}
		}
		return resultList;
	}
	
	@Override
	public List<ValueInfoObjectRecord<Double>> getValueInfoObjectRecord(
			String attrName, double queryMin, double queryMax)
	{
		LinkedList<ValueInfoObjectRecord<Double>> attrValObjList = 
				valObjTable.get(attrName);
		
		LinkedList<ValueInfoObjectRecord<Double>> resultList = 
				new LinkedList<ValueInfoObjectRecord<Double>>();
		
		for(int i=0;i<attrValObjList.size();i++)
		{
			ValueInfoObjectRecord<Double> attrValObjRec = 
					attrValObjList.get(i);
			
			// trying to find if there is an overlap in the ranges, 
			// the range specified by user and the range in database.
			// overlap is there if queryMin lies between the range in database
			// or queryMax lies between the range in database.
			// So, we specify two or conditions.
			
			boolean queryMinInBetween = (queryMin >= attrValObjRec.getRangeStart()) 
					&& (queryMin < attrValObjRec.getRangeEnd());
			
			// for right side value, it can't be equal to rangestart, 
			// but it can be equal to rangeEnd, although even then it doesn't include
			// rangeEnd.
			boolean queryMaxInBetween = (queryMax > attrValObjRec.getRangeStart()) 
					&& (queryMax <= attrValObjRec.getRangeEnd());
			
			// or on the two conditions
			if(queryMinInBetween || queryMaxInBetween)
			{
				resultList.add(attrValObjRec);
			}
		}
		return resultList;
	}
	
	@Override
	public void putAttributeMetaInfoRecord(
			AttributeMetadataInfoRecord<NodeIDType, Double> putRec)
	{
		attrMetaInfoTable.put(putRec.getAttrName(), putRec);
	}
	
	@Override
	public void putAttributeMetaObjectRecord(
			AttributeMetaObjectRecord<NodeIDType, Double> putRec,
			String attrName)
	{
		//LinkedList<AttributeMetaObjectRecord<NodeIDType, Double>>
		LinkedList<AttributeMetaObjectRecord<NodeIDType, Double>> attrMetaObjRecList 
								= this.metaObjTable.get(attrName);
		if(attrMetaObjRecList==null)
		{
			attrMetaObjRecList 
					= new LinkedList<AttributeMetaObjectRecord<NodeIDType, Double>>();
			attrMetaObjRecList.add(putRec);
			this.metaObjTable.put(attrName, attrMetaObjRecList);
		} else
		{
			attrMetaObjRecList.add(putRec);
		}
	}
	
	@Override
	public void putValueObjectRecord(ValueInfoObjectRecord<Double> putRec,
			String attrName)
	{
		//LinkedList<ValueInfoObjectRecord<Double>>
		LinkedList<ValueInfoObjectRecord<Double>> attrValObjRecList 
									= this.valObjTable.get(attrName);
		if(attrValObjRecList==null)
		{
			attrValObjRecList 
				= new LinkedList<ValueInfoObjectRecord<Double>>();
			attrValObjRecList.add(putRec);
			this.valObjTable.put(attrName, attrValObjRecList);
		} else
		{
			attrValObjRecList.add(putRec);
		}
	}
	
	@Override
	public void updateAttributeMetaObjectRecord(
			AttributeMetaObjectRecord<NodeIDType, Double> dbRec,
			String attrName, JSONObject updateValue, Operations operType,
			Keys fieldType)
	{
		switch(operType)
		{
			case APPEND:
			{
				if(fieldType == AttributeMetaObjectRecord.Keys.GROUP_GUID_LIST)
				{
					if(ContextServiceConfig.GROUP_INFO_STORAGE)
					{
						// update by reference
						JSONArray currValue = dbRec.getGroupGUIDList();
						currValue.put(updateValue);
					}
				}
				break;
			}
			case REPLACE:
			{
				break;
			}
			case REMOVE:
			{
				break;
			}
		}
	}
	
	@Override
	public void updateValueInfoObjectRecord(
			ValueInfoObjectRecord<Double> dbRec,
			String attrName,
			JSONObject updateValue,
			edu.umass.cs.contextservice.database.records.ValueInfoObjectRecord.Operations operType,
			edu.umass.cs.contextservice.database.records.ValueInfoObjectRecord.Keys fieldType)
	{
		synchronized(this.updValInfoObjRecMonitor)
		{
			switch(operType)
			{
				case APPEND:
				{
					if(fieldType == ValueInfoObjectRecord.Keys.NODE_GUID_LIST)
					{
						//update by reference
						JSONArray currValue = dbRec.getNodeGUIDList();
						currValue.put(updateValue);
					}
					break;
				}
				case REPLACE:
				{
					break;
				}
				case REMOVE:
				{
					if(fieldType == ValueInfoObjectRecord.Keys.NODE_GUID_LIST)
					{
						//update by reference
						JSONArray currValue = dbRec.getNodeGUIDList();
						// by reference entry should have been removed in this list
						removeEntryFromNodeGUIDList(currValue, updateValue);
						
						//currValue.put(updateValue);
					}
					break;
				}
			}
		}
	}
	
	private void removeEntryFromNodeGUIDList(JSONArray nodeGUIDList, JSONObject entryToBeRemoved)
	{
			try
			{
				NodeGUIDInfoRecord<Double> nodeRec = new NodeGUIDInfoRecord<Double>(entryToBeRemoved);
				int removeIndex = -1;
				for(int i=0;i<nodeGUIDList.length();i++)
				{
					NodeGUIDInfoRecord<Double> currItem = 
							new NodeGUIDInfoRecord<Double>(nodeGUIDList.getJSONObject(i));
					
					if(nodeRec.getNodeGUID().equals(currItem.getNodeGUID()))
					{
						removeIndex = i;
						break;
					}
				}
				if(removeIndex!=-1)
				{
					nodeGUIDList.remove(removeIndex);
				}
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
			//return nodeGUIDList;
	}
	
	public void printDatabase()
	{
		System.out.println("\n\n\n######################################" +
			"#########################################\n\n");
		System.out.println("Attribute metadata information "+myID);
		Set<String> keySet = metaObjTable.keySet();
		
		for(String key: keySet)
		{
			LinkedList<AttributeMetaObjectRecord<NodeIDType, Double>> 
						valueLinkedList = metaObjTable.get(key);
			
			for(int i=0;i<valueLinkedList.size();i++)
			{
				AttributeMetaObjectRecord<NodeIDType, Double> metadataObj = 
					(AttributeMetaObjectRecord<NodeIDType, Double>) valueLinkedList.get(i);
				System.out.println("NodeID "+myID+" attr "+key+" "+metadataObj.toString());
			}
		}
		
		System.out.println("\n\n\n######################################" +
			"#########################################\n\n");
		
		System.out.println("\n\n\n######################################" +
			"#########################################\n\n");
		
		System.out.println("Attribute value information "+ myID);
		
		//LinkedList<ValueInfoObjectRecord<Double>>>
		//valObjTable;
		
		keySet = valObjTable.keySet();
		
		for(String key: keySet)
		{
			LinkedList<ValueInfoObjectRecord<Double>>
						valueLinkedList = valObjTable.get(key);
			
			for(int i=0;i<valueLinkedList.size();i++)
			{
				ValueInfoObjectRecord<Double> valObj = 
					(ValueInfoObjectRecord<Double>) valueLinkedList.get(i);
				System.out.println("NodeID "+myID+" attr "+key+" "+valObj.toString());
			}
		}
		System.out.println("\n\n\n######################################" +
			"#########################################\n\n");
	}
	
	@Override
	public long getDatabaseSize()
	{
		long sizeDB = 0;
		//private final HashMap<String, AttributeMetadataInfoRecord<NodeIDType, Double>>
		//attrMetaInfoTable;

		//private final HashMap<String, LinkedList<AttributeMetaObjectRecord<NodeIDType, Double>>>
		//metaObjTable;

		//private final HashMap<String, LinkedList<ValueInfoObjectRecord<Double>>>
		//valObjTable;
		
		long attrMetaInfoTableSize = 0;
		for (String key : attrMetaInfoTable.keySet())
		{
			AttributeMetadataInfoRecord<NodeIDType, Double> obj = attrMetaInfoTable.get(key);
			
			try 
			{
				attrMetaInfoTableSize+=obj.toJSONObject().toString().length();
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
			// adding key length
			attrMetaInfoTableSize+=key.length();
		}
		
		long metaObjTableSize = 0;
		for (String key : attrMetaInfoTable.keySet())
		{
			metaObjTableSize+=key.length();
			LinkedList<AttributeMetaObjectRecord<NodeIDType, Double>> objList 
																= this.metaObjTable.get(key);
			for(int i=0;i<objList.size();i++)
			{
				AttributeMetaObjectRecord<NodeIDType, Double> attrMerObjRec = objList.get(i);
				try 
				{
					metaObjTableSize+=attrMerObjRec.toJSONObject().toString().length();
				} catch (JSONException e) 
				{
					e.printStackTrace();
				}
			}
		}
		
		long valObjTableSize = 0;
		for(String key : valObjTable.keySet())
		{
			valObjTableSize+=key.length();
			LinkedList<ValueInfoObjectRecord<Double>> valObjList = valObjTable.get(key);
			
			for(int i=0;i<valObjList.size();i++)
			{
				ValueInfoObjectRecord<Double> valObj = valObjList.get(i);
				try 
				{
					valObjTableSize+=valObj.toJSONObject().toString().length();
				} catch (JSONException e) 
				{
					e.printStackTrace();
				}
			}
		}
		sizeDB = attrMetaInfoTableSize+metaObjTableSize+valObjTableSize;
		System.out.println("DB SIZE attrMetaInfoTableSize "+attrMetaInfoTableSize+" metaObjTableSize "+metaObjTableSize
				+" valObjTableSize "+valObjTableSize +" sizeDB "+sizeDB+" complete");
		return sizeDB;
	}
	
	/*@Override
	public void putAttributeValueInfoRecord(AttributeValueInfoRecord putRec)
	{
	}*/
	
	/*@Override
	public AttributeValueInfoRecord getAttributeValueInfoRecord(String attrName)
	{
		return null;
	}*/
}