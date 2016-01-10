package edu.umass.cs.contextservice.database;

/*import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

import edu.umass.cs.contextservice.database.records.AttributeMetaObjectRecord;
import edu.umass.cs.contextservice.database.records.AttributeMetadataInfoRecord;
import edu.umass.cs.contextservice.database.records.NodeGUIDInfoRecord;
import edu.umass.cs.contextservice.database.records.ValueInfoObjectRecord;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;*/

/**
 * Class implements context service DB in mongo DB.
 * @author adipc
 *
 * @param <NodeIDType>
 */
/*public class MongoContextServiceDB<NodeIDType> extends AbstractContextServiceDB<NodeIDType>
{
	// this prefix appended by nodeID is the name of DB at each node.
	public static final String DATABASE_NAME_PREFIX					= "ContextServiceDB";
	public static final String ATTR_META_INFO_TABLE_NAME				= "AttrMetaInfoTable";
	public static final String ATTR_META_OBJ_TABLE_PREFIX				= "AttrMetaObjTable";
	//public static final String GROUP_GUID_INFO_TABLE_NAME				= "GroupGuidInfoTable";
	public static final String ATTR_VAL_INFO_TABLE_NAME				= "AttrValInfoTable";
	public static final String ATTR_VAL_OBJ_TABLE_PREFIX				= "AttrValObjTable";
	//public static final String NODE_GUID_INFO_TABLE_PREFIX			= "NodeGuidInfoTable-";
	
	// primary key key in mongoDB
	public static final String PRIMARY_KEY								= "_id";
	
	// port num
	public static final int MONGO_PORT_NUM								= 27017;
	
	// mongo client connection
	private MongoClient mongo												= null;
	
	private DB nodeDB														= null;
	
	public MongoContextServiceDB(NodeIDType myID)
	{
		super(myID);
		try
		{
			setUpDatabaseConnection();
		} catch (UnknownHostException e)
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public AttributeMetadataInfoRecord<NodeIDType, Double> getAttributeMetaInfoRecord(String attrName)
	{
		DBCollection table = nodeDB.getCollection(MongoContextServiceDB.ATTR_META_INFO_TABLE_NAME);
		
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put(AttributeMetadataInfoRecord.Keys.ATTR_NAME.toString(), attrName);
		//searchQuery.put("age", 30);
		
		DBCursor cursor = table.find(searchQuery);
		
		// there should be only one entry, describing the table name
		// and ranges of this attribute
		if(cursor.count() == 1)
		{
			while (cursor.hasNext())
			{
				DBObject dbRec = cursor.next();
				try
				{
					JSONObject recJSON = new JSONObject(dbRec.toString());
					AttributeMetadataInfoRecord<NodeIDType, Double> recObject = 
							new AttributeMetadataInfoRecord<NodeIDType, Double>(recJSON);
					return recObject;
				} catch (JSONException e)
				{
					e.printStackTrace();
				}
				//ContextServiceLogger.getLogger().fine(cursor.next());
			}
		}
		else
		{
			assert(false);
		}
		return null;
	}
	
	@Override
	public List<AttributeMetaObjectRecord<NodeIDType, Double>> 
		getAttributeMetaObjectRecord(String attrName, double queryMin, double queryMax)
	{
		DBCollection table = nodeDB.getCollection(MongoContextServiceDB.ATTR_META_OBJ_TABLE_PREFIX+attrName);
		
		//BasicDBObject searchQuery = new BasicDBObject();
		//searchQuery.put(AttributeMetaObjectRecord.Keys..toString(), attrName);
		//searchQuery.put("age", 30);
		
		//DBCursor cursor = table.find(searchQuery);
		//table.find( { field: { $gt: value1, $lt: value2 } } );
		//table.find(" { $and: [ { price: { $ne: 1.99 } }, { price: { $exists: true } } ] } ");
		//(AttributeMetaObjectRecord.Keys.RANGE_START.toString(), new BasicDBObject("$lt", queryValue).append("$lt", 5));
		
		LinkedList<AttributeMetaObjectRecord<NodeIDType, Double>> resultList = 
				new LinkedList<AttributeMetaObjectRecord<NodeIDType, Double>>();
		
		// trying to find if there is an overlap in the ranges, 
		// the range specified by user and the range in database.
		// overlap is there if queryMin lies between the range in database
		// or queryMax lies between the range in database.
		// So, we specify two or conditions.
		BasicDBObject cond1 = new BasicDBObject();
		cond1.put
		(AttributeMetaObjectRecord.Keys.RANGE_START.toString(), new BasicDBObject("$lte", queryMin));
		
		cond1.put
		(AttributeMetaObjectRecord.Keys.RANGE_END.toString(), new BasicDBObject("$gt", queryMin));
		
		// for right side value, it can't be equal to rangestart, 
		// but it can be equal to rangeEnd, although even then it doesn't include
		// rangeEnd.
		BasicDBObject cond2 = new BasicDBObject();
		cond2.put
		(AttributeMetaObjectRecord.Keys.RANGE_START.toString(), new BasicDBObject("$lt", queryMax));
		
		cond2.put
		(AttributeMetaObjectRecord.Keys.RANGE_END.toString(), new BasicDBObject("$gte", queryMax));
		
		
		BasicDBObject orQuery = new BasicDBObject();
		List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
		obj.add(cond1);
		obj.add(cond2);
		orQuery.put("$or", obj);
		
		//ContextServiceLogger.getLogger().fine(andQuery.toString());
		//DBCursor cursor = collection.find(andQuery);
		DBCursor cursor = table.find(orQuery);
		
		// there should be only one entry in which the
		// given value should lie, otherwise database is flawed
		
		while (cursor.hasNext())
		{
			DBObject dbRec = cursor.next();
			try
			{
				JSONObject recJSON = new JSONObject(dbRec.toString());
				AttributeMetaObjectRecord<NodeIDType, Double> recObject = 
						new AttributeMetaObjectRecord<NodeIDType, Double>(recJSON);
				resultList.add(recObject);
				//return recObject;
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			//ContextServiceLogger.getLogger().fine(cursor.next());
		}
		
		return resultList;
	}
	
	@Override
	public List<ValueInfoObjectRecord<Double>> getValueInfoObjectRecord(String attrName, 
			double queryMin, double queryMax)
	{
		DBCollection table = nodeDB.getCollection(MongoContextServiceDB.ATTR_VAL_OBJ_TABLE_PREFIX+attrName);
		
		//BasicDBObject searchQuery = new BasicDBObject();
		//searchQuery.put(AttributeMetaObjectRecord.Keys..toString(), attrName);
		//searchQuery.put("age", 30);
		
		//DBCursor cursor = table.find(searchQuery);
		//table.find( { field: { $gt: value1, $lt: value2 } } );
		//table.find(" { $and: [ { price: { $ne: 1.99 } }, { price: { $exists: true } } ] } ");
		//(AttributeMetaObjectRecord.Keys.RANGE_START.toString(), new BasicDBObject("$lt", queryValue).append("$lt", 5));
		
		LinkedList<ValueInfoObjectRecord<Double>> resultList = 
				new LinkedList<ValueInfoObjectRecord<Double>>();
		
		// trying to find if there is an overlap in the ranges, 
		// the range specified by user and the range in database.
		// overlap is there if queryMin lies between the range in database
		// or queryMax lies between the range in database.
		// So, we specify two or conditions.
		BasicDBObject cond1 = new BasicDBObject();
		cond1.put
		(ValueInfoObjectRecord.Keys.RANGE_START.toString(), new BasicDBObject("$lte", queryMin));
		
		cond1.put
		(ValueInfoObjectRecord.Keys.RANGE_END.toString(), new BasicDBObject("$gt", queryMin));
		
		
		// for right side value, it can't be equal to rangestart, 
		// but it can be equal to rangeEnd, although even then it doesn't include
		// rangeEnd.
		BasicDBObject cond2 = new BasicDBObject();
		cond2.put
		(ValueInfoObjectRecord.Keys.RANGE_START.toString(), new BasicDBObject("$lt", queryMax));
		
		cond2.put
		(ValueInfoObjectRecord.Keys.RANGE_END.toString(), new BasicDBObject("$gte", queryMax));
		
		
		BasicDBObject orQuery = new BasicDBObject();
		List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
		obj.add(cond1);
		obj.add(cond2);
		orQuery.put("$or", obj);
		
		//ContextServiceLogger.getLogger().fine(andQuery.toString());
		//DBCursor cursor = collection.find(andQuery);
		DBCursor cursor = table.find(orQuery);
		
		// there should be only one entry in which the
		// given value should lie, otherwise database is flawed
		
		while (cursor.hasNext())
		{
			DBObject dbRec = cursor.next();
			try
			{
				JSONObject recJSON = new JSONObject(dbRec.toString());
				ValueInfoObjectRecord<Double> recObject = 
						new ValueInfoObjectRecord<Double>(recJSON);
				resultList.add(recObject);
				//return recObject;
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			//ContextServiceLogger.getLogger().fine(cursor.next());
		}
		return resultList;
	}
	
	@Override
	public void putAttributeMetaInfoRecord(AttributeMetadataInfoRecord<NodeIDType, Double> putRec)
	{
		try
		{
			DBCollection table = nodeDB.getCollection(MongoContextServiceDB.ATTR_META_INFO_TABLE_NAME);
			DBObject dbObject = (DBObject) JSON.parse(putRec.toJSONObject().toString());
			
			table.insert(dbObject);
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public void putAttributeMetaObjectRecord(AttributeMetaObjectRecord<NodeIDType, Double> putRec, String attrName)
	{
		try
		{
			DBCollection table = nodeDB.getCollection(MongoContextServiceDB.ATTR_META_OBJ_TABLE_PREFIX
					+attrName);
			DBObject dbObject = (DBObject) JSON.parse(putRec.toJSONObject().toString());
			
			table.insert(dbObject);
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public void putValueObjectRecord(ValueInfoObjectRecord<Double> putRec, String attrName)
	{
		try
		{
			DBCollection table = nodeDB.getCollection(MongoContextServiceDB.ATTR_VAL_OBJ_TABLE_PREFIX
					+attrName);
			DBObject dbObject = (DBObject) JSON.parse(putRec.toJSONObject().toString());
			
			table.insert(dbObject);
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	public void updateAttributeMetaObjectRecord
	(AttributeMetaObjectRecord<NodeIDType, Double> dbRec, String attrName, JSONObject updateValue, 
			AttributeMetaObjectRecord.Operations operType, AttributeMetaObjectRecord.Keys fieldType)
	{
		switch(operType)
		{
			case APPEND:
			{
				if(fieldType == AttributeMetaObjectRecord.Keys.GROUP_GUID_LIST)
				{
					JSONArray currValue = dbRec.getGroupGUIDList();
					currValue.put(updateValue);
					
					DBCollection table = this.nodeDB.getCollection
							(MongoContextServiceDB.ATTR_META_OBJ_TABLE_PREFIX+attrName);
					
					BasicDBObject query = new BasicDBObject();
					ObjectId pid = null;
					try 
					{
						pid= new ObjectId(dbRec.getPrimaryKeyJSON().getString("$oid"));
						
						//query.put(MongoContextServiceDB.PRIMARY_KEY, dbRec.getPrimaryKeyJSON().toString());
						query.put(MongoContextServiceDB.PRIMARY_KEY, pid);
						
						
						BasicDBObject newDocument = new BasicDBObject();
						newDocument.put(fieldType.toString(), currValue.toString());
						
						ContextServiceLogger.getLogger().fine("fieldType "+fieldType.toString() +
								" currValue.toString() "+currValue.toString() );
						
						//DBObject dbObject = (DBObject) JSON.parse(putRec.toJSONObject().toString());
						
						BasicDBObject updateObj = new BasicDBObject();
						updateObj.put("$set", newDocument);
						
						table.update(query, updateObj);
					} catch (JSONException e)
					{
						e.printStackTrace();
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
	
	
	public void updateValueInfoObjectRecord
		(ValueInfoObjectRecord<Double> dbRec, String attrName, JSONObject updateValue, 
			ValueInfoObjectRecord.Operations operType, ValueInfoObjectRecord.Keys fieldType)
	{
		switch(operType)
		{
			case APPEND:
			{
				if(fieldType == ValueInfoObjectRecord.Keys.NODE_GUID_LIST)
				{
					JSONArray currValue = dbRec.getNodeGUIDList();
					currValue.put(updateValue);
					
					DBCollection table = this.nodeDB.getCollection
							(MongoContextServiceDB.ATTR_VAL_OBJ_TABLE_PREFIX+attrName);
					
					BasicDBObject query = new BasicDBObject();
					try 
					{
						ObjectId pid = new ObjectId(dbRec.getPrimaryKeyJSON().getString("$oid"));
						
						query.put(MongoContextServiceDB.PRIMARY_KEY, pid);
						
						BasicDBObject newDocument = new BasicDBObject();
						newDocument.put(fieldType.toString(), currValue.toString());
						
						//DBObject dbObject = (DBObject) JSON.parse(putRec.toJSONObject().toString());
						
						BasicDBObject updateObj = new BasicDBObject();
						updateObj.put("$set", newDocument);
						
						table.update(query, updateObj);
						
					} catch (JSONException e)
					{
						e.printStackTrace();
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
				if(fieldType == ValueInfoObjectRecord.Keys.NODE_GUID_LIST)
				{
					JSONArray currValue = dbRec.getNodeGUIDList();
					// by reference entry should have been removed in this list
					removeEntryFromNodeGUIDList(currValue, updateValue);
					
					//currValue.put(updateValue);
					
					DBCollection table = this.nodeDB.getCollection
							(MongoContextServiceDB.ATTR_VAL_OBJ_TABLE_PREFIX+attrName);
					
					BasicDBObject query = new BasicDBObject();
					
					try 
					{
						ObjectId pid = new ObjectId(dbRec.getPrimaryKeyJSON().getString("$oid"));
						
						//query.put(MongoContextServiceDB.PRIMARY_KEY, dbRec.getPrimaryKeyJSON().toString());
						query.put(MongoContextServiceDB.PRIMARY_KEY, pid);
						
						
						//query.put(MongoContextServiceDB.PRIMARY_KEY, dbRec.getPrimaryKeyJSON());
						
						BasicDBObject newDocument = new BasicDBObject();
						newDocument.put(fieldType.toString(), currValue.toString());
						
						//DBObject dbObject = (DBObject) JSON.parse(putRec.toJSONObject().toString());
						
						BasicDBObject updateObj = new BasicDBObject();
						updateObj.put("$set", newDocument);
						
						table.update(query, updateObj);
					} catch (JSONException e) 
					{
						e.printStackTrace();
					}
				}
				break;
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
	
	private void setUpDatabaseConnection() throws UnknownHostException
	{
		mongo = new MongoClient("localhost", MongoContextServiceDB.MONGO_PORT_NUM);
		//Mongo mongo = new Mongo("localhost", 27017);
		
		
		// if database doesn't exists, MongoDB will create it for you
		this.nodeDB = mongo.getDB(MongoContextServiceDB.DATABASE_NAME_PREFIX+this.myID);
		//DBCollection table = nodeDB.getCollection("user");
	}
	
	public void printDatabase()
	{
		
	}
	
	@Override
	public long getDatabaseSize() 
	{
		return 0;
	}

	@Override
	public boolean updateGUIDRecord(String GUID, String attrName, double value) 
	{
		return false;
	}

	@Override
	public JSONObject getGUIDRecord(String GUID) 
	{
		return new JSONObject();
	}
	
	
	/*public void updateNodeGUIDInfoRecord
	(NodeGUIDInfoRecord<Double> putRec, String attrName, Object updateValue, 
	NodeGUIDInfoRecord.Operations operType, NodeGUIDInfoRecord.Keys fieldType)
	{
		switch(operType)
		{
			case APPEND:
			{
				if(fieldType == NodeGUIDInfoRecord.Keys.NODE_GUID)
				{
					JSONArray currValue = dbRec.getGroupGUIDList();
					currValue.put(updateValue);
					
					DBCollection table = this.nodeDB.getCollection
							(MongoContextServiceDB.ATTR_META_OBJ_TABLE_PREFIX+attrName);
					
					BasicDBObject query = new BasicDBObject();
					query.put(MongoContextServiceDB.PRIMARY_KEY, dbRec.getPrimaryKeyJSON());
					
					BasicDBObject newDocument = new BasicDBObject();
					newDocument.put(fieldType.toString(), currValue.toString());
					
					//DBObject dbObject = (DBObject) JSON.parse(putRec.toJSONObject().toString());
					
					BasicDBObject updateObj = new BasicDBObject();
					updateObj.put("$set", newDocument);
					
					table.update(query, updateObj);
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
	}*/
	
	// having keys as input
	/*public void updateAttributeMetaObjectRecord
		(Double rangeStart, Double rangeEnd, String attrName, Object updateValue, 
			AttributeMetaObjectRecord.Operations operType, AttributeMetaObjectRecord.Keys fieldType)
	{
	}*/
	
	/*@Override
	public void putGroupGUIDRecord(GroupGUIDRecord putRec)
	{
		try
		{
			DBCollection table = nodeDB.getCollection(MongoContextServiceDB.GROUP_GUID_INFO_TABLE_NAME);
			DBObject dbObject = (DBObject) JSON.parse(putRec.toJSONObject().toString());
			
			table.insert(dbObject);
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public void putNodeGUIDInfoRecord(String attrName, NodeGUIDInfoRecord<?> putRec)
	{
		try
		{
			DBCollection table = nodeDB.getCollection
					(MongoContextServiceDB.NODE_GUID_INFO_TABLE_PREFIX+attrName);
			DBObject dbObject = (DBObject) JSON.parse(putRec.toJSONObject().toString());
			
			table.insert(dbObject);
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}*/
	
	/*@Override
	public GroupGUIDRecord getGroupGUIDRecord(String groupGUID)
	{
		DBCollection table = nodeDB.getCollection(MongoContextServiceDB.GROUP_GUID_INFO_TABLE_NAME);
		
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put(GroupGUIDRecord.Keys.GROUP_GUID.toString(), groupGUID);
		
		DBCursor cursor = table.find(searchQuery);
		// there should be only one entry, describing the table name
		// and ranges of this attribute
		if(cursor.count() == 1)
		{
			while (cursor.hasNext())
			{
				DBObject dbRec = cursor.next();
				try 
				{
					JSONObject recJSON = new JSONObject(dbRec.toString());
					GroupGUIDRecord recObject = 
							new GroupGUIDRecord(recJSON);
					return recObject;
				} catch (JSONException e)
				{
					e.printStackTrace();
				}
				//ContextServiceLogger.getLogger().fine(cursor.next());
			}
		}
		else
		{
			assert(false);
		}
		return null;
	}
	
	@Override
	public List<NodeGUIDInfoRecord<Double>> getNodeGUIDInfoRecord(String attrName, double queryMin, double queryMax)
	{
		DBCollection table = nodeDB.getCollection
				(MongoContextServiceDB.NODE_GUID_INFO_TABLE_PREFIX+attrName);
		
		//BasicDBObject searchQuery = new BasicDBObject();
		//searchQuery.put(NodeGUIDInfoRecord.Keys.GROUP_QUERY.toString(), groupQuery);
		
		BasicDBObject rangeQuery = new BasicDBObject();
		rangeQuery.put
		(NodeGUIDInfoRecord.Keys.ATTR_VALUE.toString(), new BasicDBObject("$gte", queryMin));
		
		rangeQuery.put
		(NodeGUIDInfoRecord.Keys.ATTR_VALUE.toString(), new BasicDBObject("$lt", queryMax));
		
		DBCursor cursor = table.find(rangeQuery);
		
		List<NodeGUIDInfoRecord<Double>> resultList 
											= new LinkedList<NodeGUIDInfoRecord<Double>>();
		
		//DBCursor cursor = table.find(searchQuery);
		// there should be only one entry, describing the table name
		// and ranges of this attribute
		while (cursor.hasNext())
		{
			DBObject dbRec = cursor.next();
			try 
			{
				JSONObject recJSON = new JSONObject(dbRec.toString());
				NodeGUIDInfoRecord<Double> recObject = 
						new NodeGUIDInfoRecord<Double>(recJSON);
				resultList.add(recObject);
				//return recObject;
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			//ContextServiceLogger.getLogger().fine(cursor.next());
		}	
		return resultList;
	}
	
	@Override
	public NodeGUIDInfoRecord<Double> getNodeGUIDInfoRecord(String attrName, String nodeGUID)
	{
		DBCollection table = nodeDB.getCollection
				(MongoContextServiceDB.NODE_GUID_INFO_TABLE_PREFIX + attrName);
		
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put(NodeGUIDInfoRecord.Keys.NODE_GUID.toString(), nodeGUID);
		
		DBCursor cursor = table.find(searchQuery);
		
		// there should be only one entry, describing the table name
		// and ranges of this attribute
		if(cursor.count() == 1)
		{
			while (cursor.hasNext())
			{
				DBObject dbRec = cursor.next();
				try
				{
					JSONObject recJSON = new JSONObject(dbRec.toString());
					NodeGUIDInfoRecord<Double> recObject = 
							new NodeGUIDInfoRecord<Double>(recJSON);
					return recObject;
				} catch (JSONException e)
				{
					e.printStackTrace();
				}
				//ContextServiceLogger.getLogger().fine(cursor.next());
			}
		}
		else
		{
			assert(false);
		}
		
		return null;
	}*/
	
	
	/*@Override
	public void putAttributeValueInfoRecord(AttributeValueInfoRecord putRec)
	{
		try
		{
			DBCollection table = nodeDB.getCollection(MongoContextServiceDB.ATTR_VAL_INFO_TABLE_NAME);
			DBObject dbObject = (DBObject) JSON.parse(putRec.toJSONObject().toString());
			
			table.insert(dbObject);
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}*/
	
	/*@Override
	public AttributeValueInfoRecord getAttributeValueInfoRecord(String attrName)
	{
		DBCollection table = nodeDB.getCollection(MongoContextServiceDB.ATTR_VAL_INFO_TABLE_NAME);
		
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put(AttributeMetadataInfoRecord.Keys.ATTR_NAME.toString(), attrName);
		//searchQuery.put("age", 30);
		
		DBCursor cursor = table.find(searchQuery);
		
		// there should be only one entry, describing the table name
		// and ranges of this attribute
		if(cursor.count() == 1)
		{
			while (cursor.hasNext())
			{
				DBObject dbRec = cursor.next();
				try 
				{
					JSONObject recJSON = new JSONObject(dbRec.toString());
					AttributeValueInfoRecord recObject = 
							new AttributeValueInfoRecord(recJSON);
					return recObject;
				} catch (JSONException e)
				{
					e.printStackTrace();
				}
				//ContextServiceLogger.getLogger().fine(cursor.next());
			}
		}
		else
		{
			assert(false);
		}
		return null;
	}
}*/