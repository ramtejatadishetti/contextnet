package edu.umass.cs.contextservice.schemes.components;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.config.ContextServiceConfig.PrivacySchemes;
import edu.umass.cs.contextservice.database.HyperspaceDB;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegion;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegionReply;
import edu.umass.cs.contextservice.messages.ValueUpdateToSubspaceRegionMessage;
import edu.umass.cs.contextservice.profilers.ProfilerStatClass;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.schemes.HyperspaceHashing;
import edu.umass.cs.contextservice.schemes.helperclasses.RegionInfoClass;
import edu.umass.cs.contextservice.updates.UpdateInfo;
import edu.umass.cs.nio.JSONMessenger;

public abstract class AbstractGUIDAttrValueProcessing
{
	protected final HashMap<Integer, Vector<SubspaceInfo>> 
																subspaceInfoMap;
	
	protected final Random replicaChoosingRand;
	
	protected final Integer myID;
	
	protected final HyperspaceDB hyperspaceDB;
	
	protected final JSONMessenger<Integer> messenger;
	
	protected final ConcurrentHashMap<Long, QueryInfo> pendingQueryRequests;
	
	protected final Object pendingQueryLock								= new Object();
	
	protected long queryIdCounter										= 0;
	
	protected final ProfilerStatClass profStats;
	
	protected final Random defaultAttrValGenerator;
	
	// FIXME: check if the abstract methods and methods implemented here are separated correctly
	public AbstractGUIDAttrValueProcessing( Integer myID, 
			HashMap<Integer, Vector<SubspaceInfo>> 
		subspaceInfoMap , HyperspaceDB hyperspaceDB, 
		JSONMessenger<Integer> messenger , 
		ConcurrentHashMap<Long, QueryInfo> pendingQueryRequests, 
		ProfilerStatClass profStats )
	{
		this.myID = myID;
		this.subspaceInfoMap = subspaceInfoMap;
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
			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo , 
			JSONObject oldValueJSON , int subspaceId , int  replicaNum ,
			JSONObject updatedAttrValJSON ,
			String GUID , long requestID , boolean firstTimeInsert,
			long updateStartTime, 
			JSONObject primarySubspaceJSON ) throws JSONException
	{
		if( firstTimeInsert )
		{
			processFirstTimeInsertIntoSecondarySubspace( 
					attrsSubspaceInfo, subspaceId , replicaNum ,
					GUID , requestID, updateStartTime, primarySubspaceJSON,  updatedAttrValJSON);
		}
		else
		{
			processUpdateIntoSecondarySubspace( 
					attrsSubspaceInfo, oldValueJSON, subspaceId, replicaNum,
					updatedAttrValJSON, GUID , requestID ,firstTimeInsert, 
					updateStartTime, primarySubspaceJSON );
		}
	}
	
	protected void processUpdateIntoSecondarySubspace( 
			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo , 
			JSONObject oldValueJSON , int subspaceId , int  replicaNum ,
			JSONObject updatedAttrValJSON ,
			String GUID , long requestID , boolean firstTimeInsert,
			long updateStartTime, JSONObject primarySubspaceJSON ) throws JSONException
	{
		Integer oldRespNodeId = null, newRespNodeId = null;
		
		// processes the first time insert
		HashMap<String, ProcessingQueryComponent> oldQueryComponents 
											= new HashMap<String, ProcessingQueryComponent>();
		
		Iterator<String> subspaceAttrIter 	= attrsSubspaceInfo.keySet().iterator();
		
		while( subspaceAttrIter.hasNext() )
		{
			String attrName = subspaceAttrIter.next();
			//( String attributeName, String leftOperator, double leftValue, 
			//		String rightOperator, double rightValue )
			String attrVal = oldValueJSON.getString(attrName);
			ProcessingQueryComponent qcomponent = new ProcessingQueryComponent
										( attrName, attrVal, attrVal );
			oldQueryComponents.put(qcomponent.getAttributeName() , qcomponent);
		}

		HashMap<Integer, RegionInfoClass> overlappingRegion = 
				this.hyperspaceDB.getOverlappingRegionsInSubspace
								(subspaceId, replicaNum, oldQueryComponents);

		assert(overlappingRegion.size() == 1);
		
		oldRespNodeId = (Integer)overlappingRegion.keySet().iterator().next();
		

		// for new value
		HashMap<String, ProcessingQueryComponent> newQueryComponents 
						= new HashMap<String, ProcessingQueryComponent>();
		
		Iterator<String> subspaceAttrIter1 
						= attrsSubspaceInfo.keySet().iterator();

		while( subspaceAttrIter1.hasNext() )
		{
			String attrName = subspaceAttrIter1.next();

			String value;
			if( updatedAttrValJSON.has(attrName) )
			{
				value = updatedAttrValJSON.getString(attrName);
			}
			else
			{
				value = oldValueJSON.getString(attrName);
			}
			ProcessingQueryComponent qcomponent 
					= new ProcessingQueryComponent(attrName, value, value );
			newQueryComponents.put(qcomponent.getAttributeName(), qcomponent);
		}
		
		HashMap<Integer, RegionInfoClass> newOverlappingRegion = 
				this.hyperspaceDB.getOverlappingRegionsInSubspace( subspaceId, replicaNum, 
						newQueryComponents );

		assert(newOverlappingRegion.size() == 1);
		
		newRespNodeId = (Integer)newOverlappingRegion.keySet().iterator().next();
		

		ContextServiceLogger.getLogger().fine
							("oldNodeId "+oldRespNodeId
							+" newRespNodeId "+newRespNodeId);
		JSONObject unsetAttrsJSON = primarySubspaceJSON.getJSONObject
				(HyperspaceDB.unsetAttrsColName);
		
		// send messages to the subspace region nodes
		if( oldRespNodeId == newRespNodeId )
		{
			// update case
			// add entry for reply
			// 1 reply as both old and new goes to same node
			// NOTE: doing this here was a bug, as we are also sending the message out
			// and sometimes replies were coming back quickly before initialization for all 
			// subspaces and the request completion code was assuming that the request was complte
			// before recv replies from all subspaces.
			//updateReq.initializeSubspaceEntry(subspaceId, replicaNum);

			ValueUpdateToSubspaceRegionMessage  
							valueUpdateToSubspaceRegionMessage = null;

			// just need to send update attr values
			valueUpdateToSubspaceRegionMessage 
					= new ValueUpdateToSubspaceRegionMessage( myID, 
							-1, GUID, updatedAttrValJSON, 
							ValueUpdateToSubspaceRegionMessage.UPDATE_ENTRY, 
							subspaceId, requestID, firstTimeInsert, updateStartTime, 
							oldValueJSON, unsetAttrsJSON, updatedAttrValJSON, PrivacySchemes.HYPERSPACE_PRIVACY.ordinal());
			
			try
			{
				this.messenger.sendToID
					(oldRespNodeId, valueUpdateToSubspaceRegionMessage.toJSONObject());
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
		else
		{
			// add entry for reply
			// 2 reply as both old and new goes to different node
			//updateReq.initializeSubspaceEntry(subspaceId, replicaNum);
			// it is a remove, so no need for update and old JSON.
			ValueUpdateToSubspaceRegionMessage  oldValueUpdateToSubspaceRegionMessage 
					= new ValueUpdateToSubspaceRegionMessage( myID, -1, 
							GUID, updatedAttrValJSON, 
							ValueUpdateToSubspaceRegionMessage.REMOVE_ENTRY, 
							subspaceId, requestID, firstTimeInsert, updateStartTime,
							oldValueJSON, unsetAttrsJSON, updatedAttrValJSON, PrivacySchemes.HYPERSPACE_PRIVACY.ordinal() );
			
			try
			{
				this.messenger.sendToID(oldRespNodeId, 
							oldValueUpdateToSubspaceRegionMessage.toJSONObject());
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			
			// FIXME: send full JSON, so that a new entry can be inserted 
			// involving attributes which were not updated.
			// need to send old JSON here, so that full guid entry can be added 
			// not just the updated attrs
			
			JSONObject jsonToWrite = new JSONObject();
			
			
			// attr values
			Iterator<String> attrIter = AttributeTypes.attributeMap.keySet().iterator();
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
			if(oldValueJSON.has(HyperspaceDB.anonymizedIDToGUIDMappingColName))
			{
				JSONArray decodingArray = oldValueJSON.getJSONArray
							(HyperspaceDB.anonymizedIDToGUIDMappingColName);
				
				assert(decodingArray!= null);
				jsonToWrite.put(HyperspaceDB.anonymizedIDToGUIDMappingColName
						, decodingArray);
			}
			
			ValueUpdateToSubspaceRegionMessage  
				newValueUpdateToSubspaceRegionMessage = 
						new ValueUpdateToSubspaceRegionMessage( myID, -1, 
								GUID, jsonToWrite,
								ValueUpdateToSubspaceRegionMessage.ADD_ENTRY, 
								subspaceId, requestID, false, updateStartTime,
								oldValueJSON, unsetAttrsJSON, updatedAttrValJSON, 
								PrivacySchemes.HYPERSPACE_PRIVACY.ordinal());
			
			try
			{
				this.messenger.sendToID(newRespNodeId, 
						newValueUpdateToSubspaceRegionMessage.toJSONObject());
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	protected void processFirstTimeInsertIntoSecondarySubspace( 
			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo,
			int subspaceId , int  replicaNum ,
			String GUID , long requestID,
			long updateStartTime, JSONObject primarySubspaceJSON, JSONObject updateAttrJSON) 
					throws JSONException
	{
		ContextServiceLogger.getLogger().fine
			("processFirstTimeInsertIntoSecondarySubspace "+primarySubspaceJSON);
		HashMap<String, ProcessingQueryComponent> newQueryComponents 
								= new HashMap<String, ProcessingQueryComponent>();
		
		Iterator<String> subspaceAttrIter
								= attrsSubspaceInfo.keySet().iterator();

		// the attribtues that are not set by the user.
		// for those random value set by primary subspace is used for indexing
		while( subspaceAttrIter.hasNext() )
		{
			String attrName = subspaceAttrIter.next();
			
			String value;
			assert(primarySubspaceJSON.has(attrName));
			
			value = primarySubspaceJSON.getString(attrName);
			
			ProcessingQueryComponent qcomponent 
				= new ProcessingQueryComponent(attrName, value, value );
			newQueryComponents.put(qcomponent.getAttributeName(), qcomponent);
		}

		HashMap<Integer, RegionInfoClass> newOverlappingRegion = 
				this.hyperspaceDB.getOverlappingRegionsInSubspace( subspaceId, replicaNum, 
								newQueryComponents );
		
		if(newOverlappingRegion.size() != 1)
		{
			ContextServiceLogger.getLogger().fine("Not 1 Assertion fail primarySubspaceJSON "
						+primarySubspaceJSON +" select query print " );
			this.hyperspaceDB.getOverlappingRegionsInSubspace( subspaceId, replicaNum, 
					newQueryComponents );
			
		}
		assert(newOverlappingRegion.size() == 1);
		
		Integer newRespNodeId 
						= (Integer)newOverlappingRegion.keySet().iterator().next();

		ContextServiceLogger.getLogger().fine
					(" newRespNodeId "+newRespNodeId);
	
		// compute the JSONToWrite
		JSONObject jsonToWrite = new JSONObject();
		
		
		JSONObject unsetAttrsJSON = primarySubspaceJSON.getJSONObject
											(HyperspaceDB.unsetAttrsColName);
		
		Iterator<String> attrIter = AttributeTypes.attributeMap.keySet().iterator();
		
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
				(HyperspaceDB.anonymizedIDToGUIDMappingColName))
		{
			JSONArray decodingArray 
				= primarySubspaceJSON.getJSONArray(HyperspaceDB.anonymizedIDToGUIDMappingColName);
			
			assert(decodingArray != null);
			jsonToWrite.put(HyperspaceDB.anonymizedIDToGUIDMappingColName, 
					decodingArray);
		}
		
		// add entry for reply
		// 1 reply as both old and new goes to same node
		// NOTE: doing this here was a bug, as we are also sending the message out
		// and sometimes replies were coming back quickly before initialization for all 
		// subspaces and the request completion code was assuming that the request was complte
		// before recv replies from all subspaces.
		//updateReq.initializeSubspaceEntry(subspaceId, replicaNum);

		ValueUpdateToSubspaceRegionMessage  
						valueUpdateToSubspaceRegionMessage = null;
		
		// need to send oldValueJSON as it is performed as insert
		valueUpdateToSubspaceRegionMessage 
				= new ValueUpdateToSubspaceRegionMessage( myID, 
							-1, GUID, jsonToWrite, 
							ValueUpdateToSubspaceRegionMessage.UPDATE_ENTRY, 
							subspaceId, requestID, true, updateStartTime, new JSONObject(),
							unsetAttrsJSON, updateAttrJSON, PrivacySchemes.HYPERSPACE_PRIVACY.ordinal());
		
		try
		{
			this.messenger.sendToID
				(newRespNodeId, valueUpdateToSubspaceRegionMessage.toJSONObject());
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
			JSONObject unsetAttrs = HyperspaceHashing.getUnsetAttrJSON(oldValJSON);
			
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
			jsonToWrite.put(HyperspaceDB.unsetAttrsColName, unsetAttrs);
			
		
			// now anonymizedIDToGUIDmapping
			
			boolean alreadyStored 
					= HyperspaceHashing.checkIfAnonymizedIDToGuidInfoAlreadyStored(oldValJSON);
			
			if( !alreadyStored )
			{
				if(anonymizedIDToGuidMapping != null)
				{
					jsonToWrite.put(HyperspaceDB.anonymizedIDToGUIDMappingColName, 
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
	
	
	public int processValueUpdateToSubspaceRegionMessage( 
			ValueUpdateToSubspaceRegionMessage valueUpdateToSubspaceRegionMessage, 
			int replicaNum )
	{
		int subspaceId  		= valueUpdateToSubspaceRegionMessage.getSubspaceNum();
		String GUID 			= valueUpdateToSubspaceRegionMessage.getGUID();
		JSONObject jsonToWrite  = valueUpdateToSubspaceRegionMessage.getJSONToWrite();
		int operType 			= valueUpdateToSubspaceRegionMessage.getOperType();
		boolean firstTimeInsert = valueUpdateToSubspaceRegionMessage.getFirstTimeInsert();
		
		long udpateStartTime 	= valueUpdateToSubspaceRegionMessage.getUpdateStartTime();
		
		String tableName 		= "subspaceId"+subspaceId+"DataStorage";
		
		int numRep = 1;
		
		try 
		{
			switch(operType)
			{
				case ValueUpdateToSubspaceRegionMessage.ADD_ENTRY:
				{
					numRep = 2;
					//if(!ContextServiceConfig.DISABLE_SECONDARY_SUBSPACES_UPDATES)
					{
						this.hyperspaceDB.storeGUIDInSecondarySubspace
							(tableName, GUID, jsonToWrite, HyperspaceDB.INSERT_REC, 
								subspaceId);
					}
					break;
				}
				case ValueUpdateToSubspaceRegionMessage.REMOVE_ENTRY:
				{
					numRep = 2;
					//if(!ContextServiceConfig.DISABLE_SECONDARY_SUBSPACES_UPDATES)
					{
						this.hyperspaceDB.deleteGUIDFromSubspaceRegion
											(tableName, GUID, subspaceId);
					}
					break;
				}
				case ValueUpdateToSubspaceRegionMessage.UPDATE_ENTRY:
				{
					numRep = 1;
					//if(!ContextServiceConfig.DISABLE_SECONDARY_SUBSPACES_UPDATES)
					{
						if( firstTimeInsert )
						{
							this.hyperspaceDB.storeGUIDInSecondarySubspace
								(tableName, GUID, jsonToWrite, HyperspaceDB.INSERT_REC, 
									subspaceId);
						}
						else
						{
							this.hyperspaceDB.storeGUIDInSecondarySubspace(tableName, GUID, 
									jsonToWrite, HyperspaceDB.UPDATE_REC, 
									subspaceId);
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
		return numRep;
	}
	
	/**
	 * Returns subspace number of the maximum overlapping
	 * subspace. Used in processing search query.
	 * @return
	 */
	protected int getMaxOverlapSubspace( HashMap<String, ProcessingQueryComponent> pqueryComponents, 
			HashMap<String, ProcessingQueryComponent> matchingAttributes )
	{
		// first the maximum matching subspace is found and then any of its replica it chosen
		Iterator<Integer> keyIter   	= subspaceInfoMap.keySet().iterator();
		int maxMatchingAttrs 			= 0;
		
		HashMap<Integer, Vector<MaxAttrMatchingStorageClass>> matchingSubspaceHashMap = 
				new HashMap<Integer, Vector<MaxAttrMatchingStorageClass>>();
		
		while( keyIter.hasNext() )
		{
			int subspaceId = keyIter.next();
			SubspaceInfo currSubInfo = subspaceInfoMap.get(subspaceId).get(0);
			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo = currSubInfo.getAttributesOfSubspace();
			
			int currMaxMatch = 0;
			HashMap<String, ProcessingQueryComponent> currMatchingComponents 
						= new HashMap<String, ProcessingQueryComponent>();
			
			Iterator<String> attrIter = pqueryComponents.keySet().iterator();
			
			while( attrIter.hasNext() )
			{
				String attrName = attrIter.next();
				ProcessingQueryComponent pqc = pqueryComponents.get(attrName);
				if( attrsSubspaceInfo.containsKey(pqc.getAttributeName()) )
				{
					currMaxMatch = currMaxMatch + 1;
					currMatchingComponents.put(pqc.getAttributeName(), pqc);
				}
			}
			
			if(currMaxMatch >= maxMatchingAttrs)
			{
				maxMatchingAttrs = currMaxMatch;
				MaxAttrMatchingStorageClass maxAttrMatchObj = new MaxAttrMatchingStorageClass();
				maxAttrMatchObj.currMatchingComponents = currMatchingComponents;
				maxAttrMatchObj.subspaceId = subspaceId;
				
				if(matchingSubspaceHashMap.containsKey(currMaxMatch))
				{
					matchingSubspaceHashMap.get(currMaxMatch).add(maxAttrMatchObj);
				}
				else
				{
					Vector<MaxAttrMatchingStorageClass> currMatchingSubspaceNumVector 
													= new Vector<MaxAttrMatchingStorageClass>();
					currMatchingSubspaceNumVector.add(maxAttrMatchObj);
					matchingSubspaceHashMap.put(currMaxMatch, currMatchingSubspaceNumVector);
				}
			}
		}
		
		Vector<MaxAttrMatchingStorageClass> maxMatchingSubspaceNumVector 
			= matchingSubspaceHashMap.get(maxMatchingAttrs);
		
		int returnIndex = replicaChoosingRand.nextInt( maxMatchingSubspaceNumVector.size() );
		matchingAttributes.clear();
		
		Iterator<String> attrIter 
				= maxMatchingSubspaceNumVector.get(returnIndex).currMatchingComponents.keySet().iterator();
		while(attrIter.hasNext())
		{
			String attrName = attrIter.next();
			matchingAttributes.put( attrName, 
			maxMatchingSubspaceNumVector.get(returnIndex).currMatchingComponents.get(attrName) );
		}
		
		
		String print = "size "+maxMatchingSubspaceNumVector.size()+" ";
		for(int i=0;i<maxMatchingSubspaceNumVector.size();i++)
		{
			print = print + maxMatchingSubspaceNumVector.get(i).subspaceId+" ";
		}
		print = print + " chosen "+maxMatchingSubspaceNumVector.get(returnIndex).subspaceId;
		
		return maxMatchingSubspaceNumVector.get(returnIndex).subspaceId;
	}
	
	protected class MaxAttrMatchingStorageClass
	{
		public int subspaceId;
		public HashMap<String, ProcessingQueryComponent> currMatchingComponents;
	}
	
	
	public abstract void processQueryMsgFromUser
		( QueryInfo queryInfo, boolean storeQueryForTrigger );
	
	public abstract void processQueryMesgToSubspaceRegionReply(QueryMesgToSubspaceRegionReply 
						queryMesgToSubspaceRegionReply);
	
	public abstract void processUpdateFromGNS( UpdateInfo updateReq );
	
	public abstract int processQueryMesgToSubspaceRegion(QueryMesgToSubspaceRegion 
														queryMesgToSubspaceRegion, JSONArray resultGUIDs);
}