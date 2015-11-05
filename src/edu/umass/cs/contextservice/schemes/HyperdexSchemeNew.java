package edu.umass.cs.contextservice.schemes;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hyperdex.client.ByteString;
import org.hyperdex.client.Client;
import org.hyperdex.client.GreaterEqual;
import org.hyperdex.client.HyperDexClientException;
import org.hyperdex.client.Iterator;
import org.hyperdex.client.LessEqual;
import org.hyperdex.client.Range;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.gns.GNSCalls;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;
import edu.umass.cs.contextservice.messages.QueryMsgToMetadataNode;
import edu.umass.cs.contextservice.messages.QueryMsgToValuenodeReply;
import edu.umass.cs.contextservice.messages.RefreshTrigger;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.messages.ValueUpdateMsgToValuenode;
import edu.umass.cs.contextservice.messages.ContextServicePacket.PacketType;
import edu.umass.cs.contextservice.processing.QueryComponent;
import edu.umass.cs.contextservice.processing.QueryInfo;
import edu.umass.cs.contextservice.processing.QueryParser;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.InterfaceNodeConfig;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;

public class HyperdexSchemeNew<NodeIDType> extends AbstractScheme<NodeIDType>
{
		public static final int QUERY_RECVD							= 1;
		public static final int UPDATE_RECVD						= 2;
		
		// all hyperdex related constants
		public static final String[] HYPERDEX_IP_ADDRESS 			= {"compute-0-23"};
		public static final int[] HYPERDEX_PORT				 		= {4999};
		
		
		
		// for details, see the key space generating python file at the end of this class.
		public static final String ATTR_SEARCH_KEYSPACE				= "contextnet";
		// guid is the key in hyperdex
		public static final String ATTR_SEARCH_KEYSPACE_KEYNAME		= "GUID";
		
		public static final String RANGE_KEYSPACE					= "rangeKeyspace";
		public static final String RANGE_KEYSPACE_KEYNAME			= "rangeKey";
		
		public static final String LOWER_RANGE_ATTRNAME				= "lowerRange";
		public static final String UPPER_RANGE_ATTRNAME				= "upperRange";
		
		public static final String ACTIVE_QUERY_MAP_NAME			= "activeQueryMap";
		
		public static final String JSONKeys_Query					= "Query";
		public static final String JSONKeys_IPPort					= "IpPort";
		public static final String JSONKeys_GrpGUID					= "GrpGUID";
		
		
		public static final int NUM_PARALLEL_CLIENTS				= 50;
		
		private final Client[] hyperdexClientArray					= new Client[NUM_PARALLEL_CLIENTS*HYPERDEX_IP_ADDRESS.length];
		
		private final ConcurrentLinkedQueue<Client> freeHClientQueue;
		
		private final Object hclientFreeMonitor						= new Object();
		
		// we don't want to do any computation in handleEvent method threads.
		private final ExecutorService nodeES;
		
		
		public HyperdexSchemeNew(InterfaceNodeConfig<NodeIDType> nc, 
				JSONMessenger<NodeIDType> m)
		{
			super(nc, m);
			
			freeHClientQueue = new ConcurrentLinkedQueue<Client>();
			nodeES = Executors.newFixedThreadPool(300);
			
			int count = 0;
			for(int j=0; j<HYPERDEX_IP_ADDRESS.length;j++)
			{
				for(int i=0;i<NUM_PARALLEL_CLIENTS;i++)
				{
					hyperdexClientArray[count] = new Client(HYPERDEX_IP_ADDRESS[j], HYPERDEX_PORT[j]);
					freeHClientQueue.add(hyperdexClientArray[count]);
					count++;
				}
			}
		}
		
		@Override
		public GenericMessagingTask<NodeIDType, ?>[] handleQueryMsgFromUser(
				ProtocolEvent<PacketType, String> event,
				ProtocolTask<NodeIDType, PacketType, String>[] ptasks)
		{
			nodeES.execute(new HandleEventThread(QUERY_RECVD, event));
			return null;
		}
		
		public GenericMessagingTask<NodeIDType,?>[] handleValueUpdateFromGNS(
				ProtocolEvent<ContextServicePacket.PacketType, String> event,
				ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
		{
			nodeES.execute(new HandleEventThread(UPDATE_RECVD, event));
			return null;
		}
		
		/**
		 * Query req received here means that
		 * no group exists in the GNS
		 * @param queryMsgFromUser
		 * @return
		 */
		public GenericMessagingTask<NodeIDType, QueryMsgToMetadataNode<NodeIDType>>[] 
				processQueryMsgFromUser(QueryMsgFromUser<NodeIDType> queryMsgFromUser)
		{
			String query = queryMsgFromUser.getQuery();
			long userReqID = queryMsgFromUser.getUserReqNum();
			String userIP = queryMsgFromUser.getSourceIP();
			int userPort = queryMsgFromUser.getSourcePort();
			
			System.out.println("QUERY RECVD QUERY_MSG recvd query recvd userReqId "+userReqID
					+"userIP "+userIP+" userPort "+userPort+" query "+query);
			
			//long queryStart = System.currentTimeMillis();
			// create the empty group in GNS
			String grpGUID = GNSCalls.createQueryGroup(query);
			JSONObject queryJSON = new JSONObject();
			try 
			{
				queryJSON.put(JSONKeys_Query, query);
				queryJSON.put(JSONKeys_IPPort, userIP+":"+userPort);
				queryJSON.put(JSONKeys_GrpGUID, grpGUID);
			} catch (JSONException e1)
			{
				e1.printStackTrace();
			}
			
			
			/*Vector<QueryComponent> qcomponents = QueryParser.parseQuery(query);
			QueryInfo<NodeIDType> currReq = new QueryInfo<NodeIDType>(query, getMyID()
					, grpGUID, userReqID, userIP, userPort, qcomponents);
			synchronized( this.pendingQueryLock )
			{
				currReq.setQueryRequestID(queryIdCounter++);
				pendingQueryRequests.put(currReq.getRequestId(), currReq);
			}*/
			
			/*Map<String, Object> predicates = getHyperdexPredicates(qcomponents);
			Iterator resultIterator = null;
			JSONArray queryAnswer = null;
			
			/*synchronized(hClientLock)
			{
				//hyperdex returns an iterator without blocking. But the iterator blocks later 
				// on while doing the iteration
				resultIterator = this.hyperdexClient.search(HYPERDEX_SPACE, predicates);
				System.out.println("Result iterator returns");
				queryAnswer = getGUIDsFromIterator(resultIterator);
			}*/
			
			Vector<QueryComponent> qcomponents = QueryParser.parseQuery(query);
			
			Map<String, Object> predicates = getHyperdexPredicates(qcomponents);
			
			JSONArray resultArray = HyperdexCalls(QUERY_RECVD, predicates, "", qcomponents, queryJSON, userReqID);
			
			
			GNSCalls.addGUIDsToGroup(resultArray, query, grpGUID);
			
			try
			{
				InetSocketAddress destAdd = new InetSocketAddress(InetAddress.getByName(userIP), userPort);
				QueryMsgFromUserReply<NodeIDType> qmesgUR = new QueryMsgFromUserReply<NodeIDType>(this.getMyID(), query, 
						grpGUID, resultArray, userReqID, resultArray.length());
				sendQueryReplyBackToUser(destAdd, qmesgUR);
			} catch (UnknownHostException e)
			{
				e.printStackTrace();
			}
			return null;
		}
		
		
		@SuppressWarnings("unchecked")
		private JSONArray getGUIDsFromIterator(Iterator hyperdexResultIterator)
		{
			JSONArray guidJSON = new JSONArray();
			
			try
			{
				while( hyperdexResultIterator.hasNext() )
				{
					Map<String, Object> wholeObjectMap 
						= (Map<String, Object>) hyperdexResultIterator.next();
					String nodeGUID = wholeObjectMap.get(ATTR_SEARCH_KEYSPACE_KEYNAME).toString();
					guidJSON.put(nodeGUID);
				}
			} catch ( HyperDexClientException e )
			{
				e.printStackTrace();
			}
			return guidJSON;
		}
		
		private Map<String, Object> getHyperdexPredicates(Vector<QueryComponent> qcomponents)
		{
			Map<String, Object> checks = new HashMap<String, Object>();
			
			for(int i=0; i<qcomponents.size(); i++)
			{
				QueryComponent currC = qcomponents.get(i);
				double lv = currC.getLeftValue();
				double rv = currC.getRightValue();
				checks.put( currC.getAttributeName(), new Range(lv, rv) );
			}
			return checks;
		}
		
		/**
		 * adds the reply of the queryComponent
		 * @throws JSONException
		 */
		private GenericMessagingTask<NodeIDType, ValueUpdateMsgToValuenode<NodeIDType>>[]
		                   processValueUpdateFromGNS(ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS)
		{
			ContextServiceLogger.getLogger().info("\n\n Recvd ValueUpdateFromGNS at " 
					+ this.getMyID() +" reply "+valUpdMsgFromGNS);
			
			long versionNum = valUpdMsgFromGNS.getVersionNum();
			String GUID = valUpdMsgFromGNS.getGUID();
			//String attrName = valUpdMsgFromGNS.getAttrName();
			String attrName = "";
			String oldVal = "";
			//String newVal = valUpdMsgFromGNS.getNewVal();
			String newVal = "";
			//JSONObject allAttrs = valUpdMsgFromGNS.getAllAttrs();
			//String sourceIP = valUpdMsgFromGNS.getSourceIP();
			//int sourcePort = valUpdMsgFromGNS.getSourcePort();
			
			Map<String, Object> attrs = new HashMap<String, Object>();
			attrs.put(attrName, Double.parseDouble(newVal));
			
			//HyperdexCalls(UPDATE_RECVD, attrs, GUID);
			HyperdexCalls(UPDATE_RECVD, attrs, GUID, null, null, versionNum);
			
			//send reply back
			//sendUpdateReplyBackToUser(sourceIP, sourcePort, versionNum );
			
			// send refresh trigger to a writer, just for experiment
			/*if( ContextServiceConfig.GROUP_UPDATE_TRIGGER )
			{
				sendRefreshReplyBackToUser("compute-0-23", 5000, 
					"groupQuery", "groupGUID", versionNum);
			}*/
			return null;
		}
		
		private JSONArray HyperdexCalls(int operType, Map<String, Object> queryOrUpdateMap, String GUID, 
				Vector<QueryComponent> queryComp, JSONObject queryJSON, long versionNum)
		{
			Client HClinetFree = null;
			
			while( HClinetFree == null )
			{
				HClinetFree = freeHClientQueue.poll();
				
				if( HClinetFree == null )
				{
					synchronized(hclientFreeMonitor)
					{
						try
						{
							hclientFreeMonitor.wait();
						} catch (InterruptedException e)
						{
							e.printStackTrace();
						}
					}
				}
			}
			
			JSONArray queryAnswer = null;
			switch(operType)
			{
				case QUERY_RECVD:
				{
					//hyperdex returns an iterator without blocking. But the iterator blocks later 
					// on while doing the iteration
					
					Iterator resultIterator = HClinetFree.search(ATTR_SEARCH_KEYSPACE, queryOrUpdateMap);
					
					queryAnswer = getGUIDsFromIterator(resultIterator);
					
					//updateGroupInformationOnQueryRecv(HClinetFree, queryComp, queryJSON);
					break;
				}
				
				case UPDATE_RECVD:
				{
					try
					{
						// sends refresh triggers.
						// done before doing actual put, as we read the values of all other attributes
						
						//updateGroupInformationOnUpdateRecv(HClinetFree, GUID, queryOrUpdateMap, 
						//		versionNum);
						
						HClinetFree.put( ATTR_SEARCH_KEYSPACE, GUID, queryOrUpdateMap );
					} catch (HyperDexClientException e)
					{
						e.printStackTrace();
					}
					break;
				}
			}
			
			synchronized(hclientFreeMonitor)
			{
				freeHClientQueue.add(HClinetFree);
				hclientFreeMonitor.notifyAll();
			}
			return queryAnswer;
		}
		
		private void updateGroupInformationOnQueryRecv(Client HClinetFree, Vector<QueryComponent> queryComp, 
				JSONObject queryJSON)
		{
			String grpGUID = "";
			try 
			{
				grpGUID = queryJSON.getString(JSONKeys_GrpGUID);
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
			
			for(int i=0; i<queryComp.size();i++)
			{
				QueryComponent currQC = queryComp.get(i);
				String attrName = currQC.getAttributeName();
				double lowerValue = currQC.getLeftValue();
				double upperValue = currQC.getRightValue();
				
				String keyspaceName = attrName+"Keyspace";
				
				// 3 queries for each predicate, only these 3 cases possible.
				Map<String, Object> query1 = new HashMap<String, Object>();
				//queryPred.lower >= lowerRange && queryPred.lower <= upperRange
				// query lower bound lies within one partition range.
				query1.put(LOWER_RANGE_ATTRNAME, new LessEqual(lowerValue));
				query1.put(UPPER_RANGE_ATTRNAME, new GreaterEqual(lowerValue));
				
				
				Map<String, Object> query2 = new HashMap<String, Object>();
				//queryPred.lower >= lowerRange && queryPred.lower <= upperRange
				// query upper bound lies within one partition range.
				query2.put(LOWER_RANGE_ATTRNAME, new LessEqual(upperValue));
				query2.put(UPPER_RANGE_ATTRNAME, new GreaterEqual(upperValue));
				
				
				Map<String, Object> query3 = new HashMap<String, Object>();
				//queryPred.lower >= lowerRange && queryPred.lower <= upperRange
				// whole parition range lies within the query lower and upper bounds
				query3.put(LOWER_RANGE_ATTRNAME, new Range(lowerValue, upperValue));
				query3.put(UPPER_RANGE_ATTRNAME, new Range(lowerValue, upperValue));
				
				
				Iterator resultIterator1 = HClinetFree.search(keyspaceName, query1);
				Iterator resultIterator2 = HClinetFree.search(keyspaceName, query2);
				Iterator resultIterator3 = HClinetFree.search(keyspaceName, query3);
				
				Map<String, Integer> unionMap = new HashMap<String, Integer>();
				
				getRangeKeyFromIterator(resultIterator1, unionMap);
				getRangeKeyFromIterator(resultIterator2, unionMap);
				getRangeKeyFromIterator(resultIterator3, unionMap);
				
				java.util.Iterator<String> rangeKeyIter = unionMap.keySet().iterator();
				
				while( rangeKeyIter.hasNext() )
				{
					try
					{
						String rangeKey = rangeKeyIter.next();
						//Map<String, Object> getMap =  HClinetFree.get(RANGE_KEYSPACE, rangeKey);
						//@SuppressWarnings("unchecked")
						//java.util.Map<String, String> activeQueryMap = 
						//		(java.util.Map<String, String>) getMap.get(ACTIVE_QUERY_MAP_NAME);
						
						java.util.Map<Object, Object> activeQueryMap = new java.util.HashMap<Object, Object>();
						
						// FIXME: right now it just overwrites any older same query
						//FIXME: synchronization problem, two threads can overwrite each other.
						activeQueryMap.put(grpGUID, queryJSON.toString());
						
						//Map<String, Object> attrs = new HashMap<String, Object>();
						//attrs.put(ACTIVE_QUERY_MAP_NAME, activeQueryMap);
						
						//HClinetFree.put(RANGE_KEYSPACE, rangeKey, attrs);
						Map<String, Map<Object, Object>> mapattributes = new HashMap<String, Map<Object, Object>>();
						mapattributes.put(ACTIVE_QUERY_MAP_NAME, activeQueryMap);
						HClinetFree.map_add(RANGE_KEYSPACE, rangeKey, mapattributes);
					}
					catch(HyperDexClientException hcex)
					{
						hcex.printStackTrace();
					}
				}
			}
		}
		
		private void updateGroupInformationOnUpdateRecv(Client HClinetFree, String GUID, 
				Map<String, Object> queryOrUpdateMap, long versionNum)
				throws HyperDexClientException
		{
			// assuming just one attribute
			java.util.Iterator<String> keyArrIter =  queryOrUpdateMap.keySet().iterator();
			String attrName = "";
			if(keyArrIter.hasNext())
			{
				attrName = keyArrIter.next();
			}
			else
			{
				assert false;
			}
			
			String keyspaceName = attrName+"Keyspace";
			
			JSONArray addedToGroups = new JSONArray();
			JSONArray removedFromGroups = new JSONArray();
			
			double newValue = (Double) queryOrUpdateMap.get(attrName);
			
			Map<String, Object> oldGUID = HClinetFree.get(ATTR_SEARCH_KEYSPACE, GUID);
			
			Map<String, Object> query1 = new HashMap<String, Object>();

			query1.put(LOWER_RANGE_ATTRNAME, new LessEqual(newValue));
			query1.put(UPPER_RANGE_ATTRNAME, new GreaterEqual(newValue));
			Iterator resultIterator1 = HClinetFree.search(keyspaceName, query1);
			
			Map<String, Integer> unionMap = new HashMap<String, Integer>();
			getRangeKeyFromIterator(resultIterator1, unionMap);
			
			java.util.Iterator<String> rangeKeyIter = unionMap.keySet().iterator();
			
			while( rangeKeyIter.hasNext() )
			{
				try
				{
					String rangeKey = rangeKeyIter.next();
					Map<String, Object> getMap =  HClinetFree.get(RANGE_KEYSPACE, rangeKey);
					@SuppressWarnings("unchecked")
					Map<ByteString, ByteString> activeQueryMap = 
							(Map<ByteString, ByteString>) getMap.get(ACTIVE_QUERY_MAP_NAME);
					
					
					java.util.Iterator<ByteString> activeIter =  activeQueryMap.keySet().iterator();
					
					while( activeIter.hasNext() )
					{
						ByteString byteStr = activeIter.next();
						String grpGUID =  byteStr.toString();
						
						//System.out.println("activeIter "+grpGUID);
						
						try
						{
							ByteString valByteStr = activeQueryMap.get(byteStr);
							JSONObject queryJSON = new JSONObject( valByteStr.toString() );
							String query = queryJSON.getString(JSONKeys_Query);
							//String ipPort = queryJSON.getString(JSONKeys_Query);
							boolean queryRes = Utils.groupMemberCheck(oldGUID, attrName, newValue, query);
							
							if(queryRes)
							{
								addedToGroups.put(queryJSON);
							}
						} catch (JSONException e)
						{
							e.printStackTrace();
						}
					}
				}
				catch(HyperDexClientException hcex)
				{
					hcex.printStackTrace();
				}
			}
			
			if( oldGUID != null )
			{
				Object oldObj = oldGUID.get(attrName);
				if( oldObj != null )
				{
					double oldVal = (Double) oldObj;
					
					Map<String, Object> query2 = new HashMap<String, Object>();

					query2.put(LOWER_RANGE_ATTRNAME, new LessEqual(oldVal));
					query2.put(UPPER_RANGE_ATTRNAME, new GreaterEqual(oldVal));
					
					Iterator resultIterator2 = HClinetFree.search(keyspaceName, query2);
					
					unionMap = new HashMap<String, Integer>();
					getRangeKeyFromIterator(resultIterator2, unionMap);
					
					rangeKeyIter = unionMap.keySet().iterator();
					
					
					while( rangeKeyIter.hasNext() )
					{
						try
						{
							String rangeKey = rangeKeyIter.next();
							Map<String, Object> getMap =  HClinetFree.get(RANGE_KEYSPACE, rangeKey);
							@SuppressWarnings("unchecked")
							Map<ByteString, ByteString> activeQueryMap = 
									(Map<ByteString, ByteString>) getMap.get(ACTIVE_QUERY_MAP_NAME);
							
							java.util.Iterator<ByteString> activeIter = activeQueryMap.keySet().iterator();
							while( activeIter.hasNext() )
							{
								ByteString getByteStr = activeIter.next();
								String grpGUID = getByteStr.toString();
								try
								{
									ByteString valStr = activeQueryMap.get(getByteStr);
									
									JSONObject queryJSON = new JSONObject( valStr.toString() );
									String query = queryJSON.getString(JSONKeys_Query);
									//String ipPort = queryJSON.getString(JSONKeys_Query);
									
									// to check if present before
									boolean queryRes1 = Utils.groupMemberCheck(oldGUID, attrName, oldVal, query);
									// but not present now
									boolean queryRes2 = Utils.groupMemberCheck(oldGUID, attrName, newValue, query);
									
									
									// if goes out of some group. 
									//present before. but not present now
									if( queryRes1 && (!queryRes2) )
									{
										removedFromGroups.put(queryJSON);
									}
									
								} catch (JSONException e)
								{
									e.printStackTrace();
								}
							}
						}
						catch(HyperDexClientException hcex)
						{
							hcex.printStackTrace();
						}
					}
				}
			}
			
			System.out.println("VersionNum "+versionNum+" addGroupLen "+addedToGroups.length()
					+" removeGroupLen "+removedFromGroups.length());
			// send refresh triggers.
			for(int i=0; i<addedToGroups.length(); i++)
			{
				try
				{
					JSONObject currObj = addedToGroups.getJSONObject(i);
					String groupQuery  = currObj.getString(JSONKeys_Query);
					String groupGUID   = currObj.getString(JSONKeys_GrpGUID);
					String ipPort      = currObj.getString(JSONKeys_IPPort);
					String [] parsed   = ipPort.split(":");
					
					InetAddress ip = InetAddress.getByName(parsed[0]);
					int port = Integer.parseInt(parsed[1]);
					
					//System.out.println("VersionNum "+versionNum+" ipAddress "+ipPort);
					
					RefreshTrigger<NodeIDType> refreshTrig 
						= new RefreshTrigger<NodeIDType>(this.getMyID(), groupQuery, groupGUID, versionNum, GUID, 
								RefreshTrigger.ADD);
					
					sendRefreshReplyBackToUser(new InetSocketAddress(ip, port), refreshTrig);
				} catch (JSONException e)
				{
					e.printStackTrace();
				} catch (UnknownHostException e) 
				{
					e.printStackTrace();
				}
			}
			
			for(int i=0; i<removedFromGroups.length(); i++)
			{
				try
				{
					JSONObject currObj = removedFromGroups.getJSONObject(i);
					String groupQuery  = currObj.getString(JSONKeys_Query);
					String groupGUID   = currObj.getString(JSONKeys_GrpGUID);
					String ipPort      = currObj.getString(JSONKeys_IPPort);
					String [] parsed   = ipPort.split(":");
					
					InetAddress ip = InetAddress.getByName(parsed[0]);
					int port = Integer.parseInt(parsed[1]);
					
					RefreshTrigger<NodeIDType> refreshTrig 
						= new RefreshTrigger<NodeIDType>(this.getMyID(), groupQuery, groupGUID, versionNum, GUID, 
								RefreshTrigger.REMOVE);
					
					sendRefreshReplyBackToUser(new InetSocketAddress(ip, port), refreshTrig);
				} catch (JSONException e)
				{
					e.printStackTrace();
				} catch (UnknownHostException e)
				{
					e.printStackTrace();
				}
			}
		}
		
		private void getRangeKeyFromIterator(Iterator resultIterator, Map<String, Integer> unionMap)
		{
			try
			{
				while( resultIterator.hasNext() )
				{
					@SuppressWarnings("unchecked")
					Map<String, Object> wholeObjectMap 
						= (Map<String, Object>) resultIterator.next();
					String rangeKey = wholeObjectMap.get(RANGE_KEYSPACE_KEYNAME).toString();
					unionMap.put(rangeKey,1);
				}
			} catch ( HyperDexClientException e )
			{
				e.printStackTrace();
			}
		}
		
		private class HandleEventThread implements Runnable
		{
			private final int eventType;
			private final ProtocolEvent<PacketType, String> event;
			
			public HandleEventThread(int eventType, ProtocolEvent<PacketType, String> event)
			{
				this.eventType = eventType;
				this.event = event;
			}
			
			@Override
			public void run()
			{
				switch(eventType)
				{
					case QUERY_RECVD:
					{
						@SuppressWarnings("unchecked")
						QueryMsgFromUser<NodeIDType> queryMsgFromUser = (QueryMsgFromUser<NodeIDType>)event;
						
						processQueryMsgFromUser(queryMsgFromUser);
						break;
					}
					case UPDATE_RECVD:
					{
						@SuppressWarnings("unchecked")
						ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS = (ValueUpdateFromGNS<NodeIDType>)event;
						
						processValueUpdateFromGNS(valUpdMsgFromGNS);
						break;
					}
				}
			}
		}
		
		
		// useless methods
		@Override
		public GenericMessagingTask<NodeIDType, ?>[] handleValueUpdateMsgToValuenodeReply(
				ProtocolEvent<PacketType, String> event,
				ProtocolTask<NodeIDType, PacketType, String>[] ptasks)
		{
			return null;
		}
		
		
		@Override
		public GenericMessagingTask<NodeIDType, ?>[] handleMetadataMsgToValuenode(
				ProtocolEvent<PacketType, String> event,
				ProtocolTask<NodeIDType, PacketType, String>[] ptasks)
		{
			return null;
		}
		
		@Override
		public GenericMessagingTask<NodeIDType, ?>[] handleQueryMsgToMetadataNode(
				ProtocolEvent<PacketType, String> event,
				ProtocolTask<NodeIDType, PacketType, String>[] ptasks)
		{
			return null;
		}
		
		@Override
		public GenericMessagingTask<NodeIDType, ?>[] handleQueryMsgToValuenode(
				ProtocolEvent<PacketType, String> event,
				ProtocolTask<NodeIDType, PacketType, String>[] ptasks)
		{
			return null;
		}
		
		@Override
		public GenericMessagingTask<NodeIDType, ?>[] handleQueryMsgToValuenodeReply(
				ProtocolEvent<PacketType, String> event,
				ProtocolTask<NodeIDType, PacketType, String>[] ptasks)
		{
			return null;
		}
		
		@Override
		public GenericMessagingTask<NodeIDType, ?>[] handleValueUpdateMsgToMetadataNode(
				ProtocolEvent<PacketType, String> event,
				ProtocolTask<NodeIDType, PacketType, String>[] ptasks)
		{
			return null;
		}
		
		@Override
		public GenericMessagingTask<NodeIDType, ?>[] handleValueUpdateMsgToValuenode(
				ProtocolEvent<PacketType, String> event,
				ProtocolTask<NodeIDType, PacketType, String>[] ptasks)
		{
			return null;
		}
		
		@Override
		public void checkQueryCompletion(QueryInfo<NodeIDType> qinfo)
		{
		}
		
		@Override
		public GenericMessagingTask<NodeIDType, ?>[] initializeScheme()
		{
			return null;
		}
		
		@Override
		protected void processReplyInternally(
				QueryMsgToValuenodeReply<NodeIDType> queryMsgToValnodeRep,
				QueryInfo<NodeIDType> queryInfo)
		{
		}
		
		@Override
		public NodeIDType getResponsibleNodeId(String AttrName)
		{
			return null;
		}

		@Override
		public GenericMessagingTask<NodeIDType, ?>[] handleBulkGet(
				ProtocolEvent<PacketType, String> event,
				ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public GenericMessagingTask<NodeIDType, ?>[] handleBulkGetReply(
				ProtocolEvent<PacketType, String> event,
				ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public GenericMessagingTask<NodeIDType, ?>[] handleConsistentStoragePut(
				ProtocolEvent<PacketType, String> event,
				ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public GenericMessagingTask<NodeIDType, ?>[] handleConsistentStoragePutReply(
				ProtocolEvent<PacketType, String> event,
				ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public GenericMessagingTask<NodeIDType, ?>[] handleQueryMesgToSubspaceRegion(
				ProtocolEvent<PacketType, String> event,
				ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public GenericMessagingTask<NodeIDType, ?>[] handleQueryMesgToSubspaceRegionReply(
				ProtocolEvent<PacketType, String> event,
				ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public GenericMessagingTask<NodeIDType, ?>[] handleValueUpdateToSubspaceRegionMessage(
				ProtocolEvent<PacketType, String> event,
				ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public GenericMessagingTask<NodeIDType, ?>[] handleGetMessage(
				ProtocolEvent<PacketType, String> event,
				ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public GenericMessagingTask<NodeIDType, ?>[] handleGetReplyMessage(
				ProtocolEvent<PacketType, String> event,
				ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
			// TODO Auto-generated method stub
			return null;
		}
}

/*
#! /usr/bin/python

import os, sys, time

numAttrs = sys.argv[1]
#lines = [line.strip() for line in open("contextServiceList.txt")]
#initialPort = 5000

writef = open("contextspace.sh", "w")

writeStr = "#!/bin/bash"+"\n"
writef.write(writeStr)

# main searchable keyspace.

writeStr = "hyperdex add-space -h compute-0-23 -p 4999 << EOF"+"\n"
writef.write(writeStr)

writeStr = "space contextnet"+"\n"
writef.write(writeStr)

writeStr = "key GUID"+"\n"
writef.write(writeStr)

writeStr = "attributes "

#     space phonebook
#     key username
#     attributes first, last, int phone
#     subspace first, last, phone
#     tolerate 0 failures
#EOF

for x in range(0, int(numAttrs)):
    name = "float contextATT"+str(x)
    writeStr = writeStr + name
    if ( not ( x == (int(numAttrs)-1) ) ):
        writeStr = writeStr + ", "
    
writeStr = writeStr + "\n"
writef.write(writeStr)

writeStr = "tolerate 0 failures"+"\n"
writef.write(writeStr)

writeStr = "EOF"+"\n"
writef.write(writeStr)

# creating range keyspaces.

for x in range(0, int(numAttrs)):
    
    time.sleep(3)
    writeStr = "hyperdex add-space -h compute-0-23 -p 4999 << EOF"+"\n"
    writef.write(writeStr)

    writeStr = "space contextATT"+str(numAttrs)+"Keyspace"+"\n"
    writef.write(writeStr)

    writeStr = "key rangeKey"+"\n"
    writef.write(writeStr)

    writeStr = "attributes float lowerRange, float upperRange, map activeQueryMap"+"\n"
    writef.write(writeStr)
    
    writeStr = "tolerate 0 failures"+"\n"
    writef.write(writeStr)

    writeStr = "EOF"+"\n"
    writef.write(writeStr)


#rangeKey keyspace
writeStr = "hyperdex add-space -h compute-0-23 -p 4999 << EOF"+"\n"
writef.write(writeStr)

writeStr = "space rangeKeyspace"+"\n"
writef.write(writeStr)

writeStr = "key rangeKey"+"\n"
writef.write(writeStr)

writeStr = "attributes map activeQueryMap"+"\n"
writef.write(writeStr)
    
writeStr = "tolerate 0 failures"+"\n"
writef.write(writeStr)

writeStr = "EOF"+"\n"
writef.write(writeStr)


writef.close()
*/