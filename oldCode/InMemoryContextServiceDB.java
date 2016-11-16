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
import edu.umass.cs.contextservice.logging.ContextServiceLogger;

/**
 * maintains inmemory database for the context service
 * @author adipc
 * @param <Integer>
 */
public class InMemoryContextServiceDB<Integer> extends AbstractContextServiceDB<Integer>
{
	//private final ConcurrentHashMap<String, AttributeMetadataInfoRecord<Integer, Double>>
	//																attrMetaInfoTable;
	
	//private final ConcurrentHashMap<String, LinkedList<AttributeMetaObjectRecord<Integer, Double>>>
	//																metaObjTable;
	
	//private final ConcurrentHashMap<String, LinkedList<ValueInfoObjectRecord<Double>>>
	//																valObjTable;
	
	
	private final HashMap<String, AttributeMetadataInfoRecord<Integer, Double>>
	attrMetaInfoTable;

	private final HashMap<String, LinkedList<AttributeMetaObjectRecord<Integer, Double>>>
	metaObjTable;

	private final HashMap<String, LinkedList<ValueInfoObjectRecord<Double>>>
	valObjTable;
	
	// no need for concurrency, there are no removals or iterator
	// used to store GUID record
	private final HashMap<String, JSONObject>  GUIDStorageTable;
	
	private final Object updValInfoObjRecMonitor;
	
	private final Object updateGUIDRecMonitor										= new Object();
	
	
	public InMemoryContextServiceDB(Integer myID)
	{
		super(myID);
		//attrMetaInfoTable 	= new ConcurrentHashMap<String, AttributeMetadataInfoRecord<Integer, Double>>();
		//metaObjTable 		= new ConcurrentHashMap<String, LinkedList<AttributeMetaObjectRecord<Integer, Double>>>();
		//valObjTable 		= new ConcurrentHashMap<String, LinkedList<ValueInfoObjectRecord<Double>>>();
		
		attrMetaInfoTable 	= new HashMap<String, AttributeMetadataInfoRecord<Integer, Double>>();
		metaObjTable 		= new HashMap<String, LinkedList<AttributeMetaObjectRecord<Integer, Double>>>();
		valObjTable 		= new HashMap<String, LinkedList<ValueInfoObjectRecord<Double>>>();
		
		// separately synchronized
		GUIDStorageTable = new HashMap<String, JSONObject>();
		
		// sync monintors
		updValInfoObjRecMonitor = new Object();
	}
	
	@Override
	public AttributeMetadataInfoRecord<Integer, Double> getAttributeMetaInfoRecord(
			String attrName)
	{
		return attrMetaInfoTable.get(attrName);
	}
	
	@Override
	public List<AttributeMetaObjectRecord<Integer, Double>> getAttributeMetaObjectRecord(
			String attrName, double queryMin, double queryMax)
	{
		LinkedList<AttributeMetaObjectRecord<Integer, Double>> attrMetaObjList = 
				metaObjTable.get(attrName);
		
		LinkedList<AttributeMetaObjectRecord<Integer, Double>> resultList = 
				new LinkedList<AttributeMetaObjectRecord<Integer, Double>>();
		
		for(int i=0;i<attrMetaObjList.size();i++)
		{
			AttributeMetaObjectRecord<Integer, Double> attrMetaObjRec = 
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
			
			// or the range lies in between the queryMin and queryMax
			
			boolean rangeInBetween = (queryMin <= attrMetaObjRec.getRangeStart()) 
					&& (queryMax > attrMetaObjRec.getRangeEnd());
			
			// or on the two conditions
			if(queryMinInBetween || queryMaxInBetween || rangeInBetween)
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
		ContextServiceLogger.getLogger().fine("getValueInfoObjectRecord attrName "+attrName+" queryMin "+queryMin
				+" queryMax "+queryMax);
		
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
			
			boolean rangeInBetween = (queryMin <= attrValObjRec.getRangeStart()) 
					&& (queryMax > attrValObjRec.getRangeEnd());
			
			// or on the two conditions
			if(queryMinInBetween || queryMaxInBetween || rangeInBetween)
			{
				resultList.add(attrValObjRec);
			}
		}
		return resultList;
	}
	
	@Override
	public void putAttributeMetaInfoRecord(
			AttributeMetadataInfoRecord<Integer, Double> putRec)
	{
		attrMetaInfoTable.put(putRec.getAttrName(), putRec);
	}
	
	@Override
	public void putAttributeMetaObjectRecord(
			AttributeMetaObjectRecord<Integer, Double> putRec,
			String attrName)
	{
		//LinkedList<AttributeMetaObjectRecord<Integer, Double>>
		LinkedList<AttributeMetaObjectRecord<Integer, Double>> attrMetaObjRecList 
								= this.metaObjTable.get(attrName);
		if(attrMetaObjRecList==null)
		{
			attrMetaObjRecList 
					= new LinkedList<AttributeMetaObjectRecord<Integer, Double>>();
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
	
	//FIXME: need to get groupGUID from JSON object
	@Override
	public void updateAttributeMetaObjectRecord(
			AttributeMetaObjectRecord<Integer, Double> dbRec,
			String attrName, JSONObject updateValue, Operations operType,
			Keys fieldType)
	{
		switch(operType)
		{
			case APPEND:
			{
				if(fieldType == AttributeMetaObjectRecord.Keys.GROUP_GUID_LIST)
				{
					if(ContextServiceConfig.GROUP_INFO_COMPONENT)
					{
						// update by reference
						JSONArray currValue = dbRec.getGroupGUIDList();
						currValue.put(updateValue);
						ContextServiceLogger.getLogger().fine(InMemoryContextServiceDB.class.getName()
								+": Groups GUID list size after inserting "+currValue.length());
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
						ContextServiceLogger.getLogger().fine("updateValueInfoObjectRecord APPEND updateValue "+updateValue
								+" updated "+currValue);
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
						
						ContextServiceLogger.getLogger().fine("updateValueInfoObjectRecord REMOVE updateValue "+updateValue
								+" updated "+currValue);
						
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
		ContextServiceLogger.getLogger().fine("\n\n\n######################################" +
			"#########################################\n\n");
		ContextServiceLogger.getLogger().fine("Attribute metadata information "+myID);
		Set<String> keySet = metaObjTable.keySet();
		
		for(String key: keySet)
		{
			LinkedList<AttributeMetaObjectRecord<Integer, Double>> 
						valueLinkedList = metaObjTable.get(key);
			
			for(int i=0;i<valueLinkedList.size();i++)
			{
				AttributeMetaObjectRecord<Integer, Double> metadataObj = 
					(AttributeMetaObjectRecord<Integer, Double>) valueLinkedList.get(i);
				ContextServiceLogger.getLogger().fine("NodeID "+myID+" attr "+key+" "+metadataObj.toString());
			}
		}
		
		ContextServiceLogger.getLogger().fine("\n\n\n######################################" +
			"#########################################\n\n");
		
		ContextServiceLogger.getLogger().fine("\n\n\n######################################" +
			"#########################################\n\n");
		
		ContextServiceLogger.getLogger().fine("Attribute value information "+ myID);
		
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
				ContextServiceLogger.getLogger().fine("NodeID "+myID+" attr "+key+" "+valObj.toString());
			}
		}
		ContextServiceLogger.getLogger().fine("\n\n\n######################################" +
			"#########################################\n\n");
	}
	
	@Override
	public long getDatabaseSize()
	{
		long sizeDB = 0;
		
		long attrMetaInfoTableSize = 0;
		for (String key : attrMetaInfoTable.keySet())
		{
			AttributeMetadataInfoRecord<Integer, Double> obj = attrMetaInfoTable.get(key);
			
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
			LinkedList<AttributeMetaObjectRecord<Integer, Double>> objList 
																= this.metaObjTable.get(key);
			for(int i=0;i<objList.size();i++)
			{
				AttributeMetaObjectRecord<Integer, Double> attrMerObjRec = objList.get(i);
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
		ContextServiceLogger.getLogger().fine("DB SIZE attrMetaInfoTableSize "+attrMetaInfoTableSize+" metaObjTableSize "+metaObjTableSize
				+" valObjTableSize "+valObjTableSize +" sizeDB "+sizeDB+" complete");
		return sizeDB;
	}
	
	@Override
	public boolean updateGUIDRecord(String GUID, String attrName, double value)
	{
		synchronized(this.updateGUIDRecMonitor)
		{
			JSONObject guidRec = this.GUIDStorageTable.get(GUID);
			
			if( guidRec == null )
			{
				guidRec = new JSONObject();
				try 
				{
					guidRec.put(attrName, value);
					this.GUIDStorageTable.put(GUID, guidRec);
				} catch (JSONException e) 
				{
					e.printStackTrace();
					return false;
				}
			}
			else
			{
				try 
				{
					guidRec.put(attrName, value);
				} catch (JSONException e) 
				{
					e.printStackTrace();
					return false;
				}
			}
		}
		return true;
	}
	
	@Override
	public JSONObject getGUIDRecord(String GUID)
	{
		return this.GUIDStorageTable.get(GUID);
	}
}