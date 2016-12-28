package edu.umass.cs.contextservice.schemes.components;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.AbstractDataStorageDB;
import edu.umass.cs.contextservice.database.RegionMappingDataStorageDB;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegion;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegionReply;
import edu.umass.cs.contextservice.messages.ValueUpdateToSubspaceRegionMessage;
import edu.umass.cs.contextservice.profilers.ProfilerStatClass;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.regionmapper.AbstractRegionMappingPolicy;
import edu.umass.cs.contextservice.regionmapper.AbstractRegionMappingPolicy.REQUEST_TYPE;
import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;
import edu.umass.cs.contextservice.schemes.RegionMappingBasedScheme;
import edu.umass.cs.contextservice.updates.UpdateInfo;
import edu.umass.cs.nio.JSONMessenger;

public abstract class AbstractGUIDAttrValueProcessing
{
//	protected final HashMap<Integer, Vector<SubspaceInfo>> 
//																subspaceInfoMap;
	
	protected final AbstractRegionMappingPolicy regionMappingPolicy;
	protected final Random replicaChoosingRand;
	
	protected final Integer myID;
	
	protected final AbstractDataStorageDB hyperspaceDB;
	
	protected final JSONMessenger<Integer> messenger;
	
	protected final ConcurrentHashMap<Long, QueryInfo> pendingQueryRequests;
	
	protected final Object pendingQueryLock								= new Object();
	
	protected long queryIdCounter										= 0;
	
	protected final ProfilerStatClass profStats;
	
	protected final Random defaultAttrValGenerator;
	
	// FIXME: check if the abstract methods and methods implemented here are separated correctly
	public AbstractGUIDAttrValueProcessing( Integer myID, 
			AbstractRegionMappingPolicy regionMappingPolicy, 
			AbstractDataStorageDB hyperspaceDB, JSONMessenger<Integer> messenger , 
		ConcurrentHashMap<Long, QueryInfo> pendingQueryRequests, 
		ProfilerStatClass profStats )
	{
		this.myID = myID;
		this.regionMappingPolicy = regionMappingPolicy;
		this.hyperspaceDB = hyperspaceDB;
		this.messenger = messenger;
		this.pendingQueryRequests = pendingQueryRequests;
		this.profStats = profStats;
		replicaChoosingRand = new Random(myID.hashCode());
		
		defaultAttrValGenerator = new Random(myID.hashCode());
	}
	
	protected boolean checkOverlapWithSubspaceAttrs(
			HashMap<String, AttributePartitionInfo> attributesOfSubspace, 
				JSONArray attrSetOfAnonymizedID )
	{
		boolean overlapWithSubspace = false;
		
		
		for(int i=0; i < attrSetOfAnonymizedID.length(); i++)
		{
			try 
			{
				String attrName = attrSetOfAnonymizedID.getString(i);
				
				if( attributesOfSubspace.containsKey(attrName) )
				{
					overlapWithSubspace = true;
					break;
				}
				
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
		
		return overlapWithSubspace;
	}
	
	protected void guidValueProcessingOnUpdate( 
			JSONObject oldValueJSON, JSONObject updatedAttrValJSON ,
			String GUID , long requestID , boolean firstTimeInsert,
			long updateStartTime, JSONObject primarySubspaceJSON, 
			UpdateInfo updateReq ) throws JSONException
	{
		if( firstTimeInsert )
		{
			processFirstTimeInsertIntoSecondarySubspace( 
					GUID , requestID, updateStartTime, primarySubspaceJSON, 
					updatedAttrValJSON, updateReq);
		}
		else
		{
			processUpdateIntoSecondarySubspace( oldValueJSON, updatedAttrValJSON, 
					GUID , requestID ,firstTimeInsert, 
					updateStartTime, primarySubspaceJSON, 
					updateReq );
		}
	}
	
	protected void processUpdateIntoSecondarySubspace(
			JSONObject oldValueJSON, JSONObject updatedAttrValJSON ,
			String GUID , long requestID , boolean firstTimeInsert,
			long updateStartTime, JSONObject primarySubspaceJSON, 
			UpdateInfo updateReq ) throws JSONException
	{
		Iterator<String> attrIter = AttributeTypes.attributeMap.keySet().iterator();
		ValueSpaceInfo oldValSpace = new ValueSpaceInfo();
		
		while(attrIter.hasNext())
		{
			String attrName = attrIter.next();
			String attrVal  = oldValueJSON.getString(attrName);
			
			// lower and upper bound are same in updates.
			AttributeValueRange attrValRange = new AttributeValueRange(attrVal, attrVal);
			oldValSpace.getValueSpaceBoundary().put(attrName, attrValRange);
		}
		
		List<Integer> oldValSpaceList 
					= regionMappingPolicy.getNodeIDsForAValueSpace(oldValSpace, REQUEST_TYPE.UPDATE);
		
		
		// for new value
		attrIter = AttributeTypes.attributeMap.keySet().iterator();
		ValueSpaceInfo newValSpace = new ValueSpaceInfo();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			
			String value;
			if( updatedAttrValJSON.has(attrName) )
			{
				value = updatedAttrValJSON.getString(attrName);
			}
			else
			{
				value = oldValueJSON.getString(attrName);
			}
			
			// lower and upper bound are same in updates.
			AttributeValueRange attrValRange = new AttributeValueRange(value, value);
			newValSpace.getValueSpaceBoundary().put(attrName, attrValRange);
		}
		
		List<Integer> newValSpaceList 
					= regionMappingPolicy.getNodeIDsForAValueSpace(newValSpace, REQUEST_TYPE.UPDATE);
		
		HashMap<Integer, Integer> removeNodesMap = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> addNodesMap = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> updateNodesMap = new HashMap<Integer, Integer>();
		
		
		for(int i=0; i<oldValSpaceList.size(); i++)
		{
			int nodeId = oldValSpaceList.get(i);
			removeNodesMap.put(nodeId, nodeId);
		}
		
		
		for(int i=0; i<newValSpaceList.size(); i++)
		{
			int nodeId = newValSpaceList.get(i);
			addNodesMap.put(nodeId, nodeId);			
		}
		
		Iterator<Integer> nodeIter = removeNodesMap.keySet().iterator();
		
		while(nodeIter.hasNext())
		{
			int nodeid = nodeIter.next();
			
			if(addNodesMap.containsKey(nodeid))
			{
				// then this is the case for update.
				updateNodesMap.put(nodeid, nodeid);
			}
		}
		
		// remove update nodes from add and remove nodes map
		nodeIter = updateNodesMap.keySet().iterator();
		while( nodeIter.hasNext() )
		{
			int nodeid = nodeIter.next();
			addNodesMap.remove(nodeid);
			removeNodesMap.remove(nodeid);
		}
		
		int totalExpectedUpdateReplies = addNodesMap.size() + removeNodesMap.size() + updateNodesMap.size();
		
		updateReq.setNumberOfExpectedReplies(totalExpectedUpdateReplies);
		
		if(ContextServiceConfig.PROFILER_THREAD)
		{
			profStats.incrementNumUpdates(totalExpectedUpdateReplies);
		}
		
		
		JSONObject unsetAttrsJSON = primarySubspaceJSON.getJSONObject
				(RegionMappingDataStorageDB.unsetAttrsColName);
		
		// sending remove
		nodeIter = removeNodesMap.keySet().iterator();
		
		while( nodeIter.hasNext() )
		{
			int nodeid = nodeIter.next();
			
			// it is a remove, so no need for update and old JSON.
			
			ValueUpdateToSubspaceRegionMessage  oldValueUpdateToSubspaceRegionMessage 
							= new ValueUpdateToSubspaceRegionMessage( myID, -1, 
										GUID, updatedAttrValJSON, 
										ValueUpdateToSubspaceRegionMessage.REMOVE_ENTRY
										, requestID, firstTimeInsert, updateStartTime,
										oldValueJSON, unsetAttrsJSON, updatedAttrValJSON, 
										ContextServiceConfig.privacyScheme.ordinal() );
			
			try
			{
				this.messenger.sendToID(nodeid, 
							oldValueUpdateToSubspaceRegionMessage.toJSONObject());
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		// sending add
		nodeIter = addNodesMap.keySet().iterator();
		while( nodeIter.hasNext() )
		{
			int nodeid = nodeIter.next();
			
			JSONObject jsonToWrite = new JSONObject();
						
						
			// attr values
			attrIter = AttributeTypes.attributeMap.keySet().iterator();
			while( attrIter.hasNext() )
			{
				String attrName = attrIter.next();
				String attrVal = "";
							
				if(updatedAttrValJSON.has(attrName))
				{
					attrVal = updatedAttrValJSON.getString(attrName);
				}
				else
				{
					if(unsetAttrsJSON.has(attrName))
					{
						attrVal = AttributeTypes.attributeMap.get(attrName).getDefaultValue();
					}
					else
					{
						assert(oldValueJSON.has(attrName));
						attrVal = oldValueJSON.getString(attrName);
					}
				}
				jsonToWrite.put(attrName, attrVal);
			}
						
			// anonymizedIDToGUID mapping
			if(oldValueJSON.has(RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName))
			{
				JSONArray decodingArray = oldValueJSON.getJSONArray
							(RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName);
				
				assert(decodingArray!= null);
				jsonToWrite.put(RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName
						, decodingArray);
			}
			
			//FIXME: need to check if sending Hyperspace privacy is correct here.
			ValueUpdateToSubspaceRegionMessage  
				newValueUpdateToSubspaceRegionMessage = 
						new ValueUpdateToSubspaceRegionMessage( myID, -1, 
								GUID, jsonToWrite,
								ValueUpdateToSubspaceRegionMessage.ADD_ENTRY, 
								requestID, false, updateStartTime,
								oldValueJSON, unsetAttrsJSON, updatedAttrValJSON, 
								ContextServiceConfig.privacyScheme.ordinal());
			
			try
			{
				this.messenger.sendToID(nodeid, 
						newValueUpdateToSubspaceRegionMessage.toJSONObject());
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		// sending updates
		nodeIter =  updateNodesMap.keySet().iterator();
		while( nodeIter.hasNext() )
		{
			int nodeid = nodeIter.next();
			
			ValueUpdateToSubspaceRegionMessage  
										valueUpdateToSubspaceRegionMessage = null;

			// just need to send update attr values
			valueUpdateToSubspaceRegionMessage 
								= new ValueUpdateToSubspaceRegionMessage( myID, 
										-1, GUID, updatedAttrValJSON, 
										ValueUpdateToSubspaceRegionMessage.UPDATE_ENTRY, 
										requestID, firstTimeInsert, updateStartTime, 
										oldValueJSON, unsetAttrsJSON, updatedAttrValJSON, 
										ContextServiceConfig.privacyScheme.ordinal());
			
			try
			{
				this.messenger.sendToID
					(nodeid, valueUpdateToSubspaceRegionMessage.toJSONObject());
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			catch (JSONException e)
			{
				e.printStackTrace();
			}			
		}
		
	}
	
	
	protected void processFirstTimeInsertIntoSecondarySubspace(
			String GUID , long requestID, long updateStartTime, 
			JSONObject primarySubspaceJSON, JSONObject updateAttrJSON, 
			UpdateInfo updateReq) throws JSONException
	{
		ContextServiceLogger.getLogger().fine
			("processFirstTimeInsertIntoSecondarySubspace "+primarySubspaceJSON);
		
		ValueSpaceInfo newValSpace = new ValueSpaceInfo();
		
		Iterator<String> attrIter
								= AttributeTypes.attributeMap.keySet().iterator();

		// the attribtues that are not set by the user.
		// for those random value set by primary subspace is used for indexing
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			String value;
			assert(primarySubspaceJSON.has(attrName));
			
			value = primarySubspaceJSON.getString(attrName);
			
			AttributeValueRange attrValRange = new AttributeValueRange(value, value);
			
			newValSpace.getValueSpaceBoundary().put(attrName, attrValRange);
		}
		
		List<Integer> newNodeList = regionMappingPolicy.getNodeIDsForAValueSpace(newValSpace, REQUEST_TYPE.UPDATE);
		
		updateReq.setNumberOfExpectedReplies(newNodeList.size());
		
		if(ContextServiceConfig.PROFILER_THREAD)
		{
			profStats.incrementNumUpdates(newNodeList.size());
		}
	
		// compute the JSONToWrite
		JSONObject jsonToWrite = new JSONObject();
		
		JSONObject unsetAttrsJSON = primarySubspaceJSON.getJSONObject
											(RegionMappingDataStorageDB.unsetAttrsColName);
		
		attrIter = AttributeTypes.attributeMap.keySet().iterator();
		
		// unset attributes are set to default values, so that they don't 
		// satisfy query constraints. But they are set some random value in primary subspace
		// and they are indexed based on these random values in secondary subspaces.
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			assert(primarySubspaceJSON.has(attrName));
			String attrVal = "";
			if(unsetAttrsJSON.has(attrName))
			{
				attrVal = AttributeTypes.attributeMap.get(attrName).getDefaultValue();
			}
			else
			{
				attrVal = primarySubspaceJSON.getString(attrName);
			}
			
			jsonToWrite.put(attrName, attrVal);
		}
		
		// add the anonymizedIDToGUID mapping if it is there
		if(primarySubspaceJSON.has
				(RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName))
		{
			JSONArray decodingArray 
				= primarySubspaceJSON.getJSONArray(RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName);
			
			assert(decodingArray != null);
			jsonToWrite.put(RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName, 
					decodingArray);
		}
		
		
		for(int i=0; i<newNodeList.size(); i++)
		{
			int nodeid = newNodeList.get(i);
			
			ValueUpdateToSubspaceRegionMessage  
							valueUpdateToSubspaceRegionMessage = null;
			
			// need to send oldValueJSON as it is performed as insert
			valueUpdateToSubspaceRegionMessage 
					= new ValueUpdateToSubspaceRegionMessage( myID, 
								-1, GUID, jsonToWrite, 
								ValueUpdateToSubspaceRegionMessage.ADD_ENTRY, 
								requestID, true, updateStartTime, new JSONObject(),
								unsetAttrsJSON, updateAttrJSON, ContextServiceConfig.privacyScheme.ordinal());
			
			try
			{
				this.messenger.sendToID
					(nodeid, valueUpdateToSubspaceRegionMessage.toJSONObject());
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
	}
	
	protected JSONObject getJSONToWriteInPrimarySubspace( JSONObject oldValJSON, 
			JSONObject updateValJSON, JSONArray anonymizedIDToGuidMapping )
	{
		JSONObject jsonToWrite = new JSONObject();
		
		// set the attributes.
		try
		{
			// attributes which are not set should be set to default value
			// for attribute-space hashing
			Iterator<String> attrIter 
							= AttributeTypes.attributeMap.keySet().iterator();
			
			while( attrIter.hasNext() )
			{
				String attrName = attrIter.next();
				AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
				String attrVal = "";
				
				if( updateValJSON.has(attrName) )
				{
					attrVal = updateValJSON.getString(attrName);
					jsonToWrite.put(attrName, attrVal);
				}
				else if( !oldValJSON.has(attrName) )
				{
					attrVal = attrMetaInfo.getARandomValue
									(this.defaultAttrValGenerator);
					jsonToWrite.put(attrName, attrVal);
				}
			}
		
			// update unset attributes
			JSONObject unsetAttrs = RegionMappingBasedScheme.getUnsetAttrJSON(oldValJSON);
			
			if( unsetAttrs != null )
			{
				// JSON iterator warning
				@SuppressWarnings("unchecked")
				Iterator<String> updateAttrIter = updateValJSON.keys();
				
				while( updateAttrIter.hasNext() )
				{
					String updateAttr = updateAttrIter.next();
					// just removing attr that is set in this update.
					unsetAttrs.remove(updateAttr);					
				}
			}
			else
			{
				unsetAttrs = new JSONObject();
			
				attrIter = AttributeTypes.attributeMap.keySet().iterator();
			
				while( attrIter.hasNext() )
				{
					String attrName = attrIter.next();
					
					if( !updateValJSON.has(attrName) )
					{
						// just "" string for minium space usage.
						unsetAttrs.put(attrName, "");
					}
				}
			}
			assert(unsetAttrs != null);
			jsonToWrite.put(RegionMappingDataStorageDB.unsetAttrsColName, unsetAttrs);
			
		
			// now anonymizedIDToGUIDmapping
			
			boolean alreadyStored 
					= RegionMappingBasedScheme.checkIfAnonymizedIDToGuidInfoAlreadyStored(oldValJSON);
			
			if( !alreadyStored )
			{
				if(anonymizedIDToGuidMapping != null)
				{
					jsonToWrite.put(RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName, 
						anonymizedIDToGuidMapping);
				}
			}
			
			return jsonToWrite;
		}
		catch( Error | Exception ex )
		{
			ex.printStackTrace();
		}
		return null;
	}
	
	
	public void processValueUpdateToSubspaceRegionMessage( 
			ValueUpdateToSubspaceRegionMessage valueUpdateToSubspaceRegionMessage )
	{
		String GUID 			= valueUpdateToSubspaceRegionMessage.getGUID();
		JSONObject jsonToWrite  = valueUpdateToSubspaceRegionMessage.getJSONToWrite();
		int operType 			= valueUpdateToSubspaceRegionMessage.getOperType();
		boolean firstTimeInsert = valueUpdateToSubspaceRegionMessage.getFirstTimeInsert();
		
		long udpateStartTime 	= valueUpdateToSubspaceRegionMessage.getUpdateStartTime();
		
		String tableName 		= RegionMappingDataStorageDB.ATTR_INDEX_TABLE_NAME;
		
		try 
		{
			switch(operType)
			{
				case ValueUpdateToSubspaceRegionMessage.ADD_ENTRY:
				{
					//if(!ContextServiceConfig.DISABLE_SECONDARY_SUBSPACES_UPDATES)
					{
						this.hyperspaceDB.storeGUIDUsingAttrIndex
							(tableName, GUID, jsonToWrite, RegionMappingDataStorageDB.INSERT_REC);
					}
					break;
				}
				case ValueUpdateToSubspaceRegionMessage.REMOVE_ENTRY:
				{
					//if(!ContextServiceConfig.DISABLE_SECONDARY_SUBSPACES_UPDATES)
					{
						this.hyperspaceDB.deleteGUIDFromTable(tableName, GUID);
					}
					break;
				}
				case ValueUpdateToSubspaceRegionMessage.UPDATE_ENTRY:
				{
					//if(!ContextServiceConfig.DISABLE_SECONDARY_SUBSPACES_UPDATES)
					{
						if( firstTimeInsert )
						{
							this.hyperspaceDB.storeGUIDUsingAttrIndex
									(tableName, GUID, jsonToWrite, RegionMappingDataStorageDB.INSERT_REC);
						}
						else
						{
							this.hyperspaceDB.storeGUIDUsingAttrIndex
									(tableName, GUID, jsonToWrite, RegionMappingDataStorageDB.UPDATE_REC);
						}
					}
					break;
				}
			}
			
			if(ContextServiceConfig.DEBUG_MODE)
			{
				long now = System.currentTimeMillis();
				System.out.println("processValueUpdateToSubspaceRegionMessage completes "
						+(now-udpateStartTime));
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	
	public abstract void processQueryMsgFromUser
		( QueryInfo queryInfo, boolean storeQueryForTrigger );
	
	public abstract void processQueryMesgToSubspaceRegionReply(QueryMesgToSubspaceRegionReply 
						queryMesgToSubspaceRegionReply);
	
	public abstract void processUpdateFromGNS( UpdateInfo updateReq );
	
	public abstract int processQueryMesgToSubspaceRegion(QueryMesgToSubspaceRegion 
														queryMesgToSubspaceRegion, JSONArray resultGUIDs);
}