package edu.umass.cs.contextservice.queryparsing;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Vector;

import org.json.JSONArray;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.records.OverlappingInfoClass;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegionReply;

/**
 * Class to store the query related information, 
 * like query, its source etc
 * @author ayadav
 */
public class QueryInfo<NodeIDType>
{
	// if a query is a where or a join query
	public static final int WHERE_QUERY								= 1;
	public static final int JOIN_QUERY								= 2;
	
	// user query
	private final String searchQuery;
	private  NodeIDType sourceNodeId;
	private  long requestId;
	private  String groupGUID;
	
	// req id set by the user
	private  long userReqID;
	private  String userIP;
	private  int userPort;
	
	private long expiryTime;
	
	
	//private final AbstractScheme<NodeIDType> scheme;
	// stores the parsed query components
	public Vector<QueryComponent> queryComponents;
	
	// indexed by attrName
	public HashMap<String, ProcessingQueryComponent> processingQueryComponents;
	// stores the replies recvd from the value nodes for the query
	// Hash map indexed by componentId, and Vector<String> stores 
	// the GUIDs
	public HashMap<Integer, LinkedList<String>> componentReplies;
	//public HashMap<Integer, JSONArray> componentReplies;
	
	private JSONArray hyperdexResultArray;
	
	// for synch
	private boolean requestCompl;
	
	// to store replies of each region of subspace
	public HashMap<Integer, OverlappingInfoClass> regionalReplies;
	public HashMap<Integer, Integer> regionalRepliesSize;
	private final Object regionalRepliesLock = new Object();
	private int regionalRepliesCounter 		 = 0;
	
	
	public QueryInfo( String query, 
			NodeIDType sourceNodeId, String grpGUID, long userReqID, 
			String userIP, int userPort, long expiryTime )
	{
		this.searchQuery = query;
		this.sourceNodeId = sourceNodeId;
		//this.requestId = requestID;
		this.groupGUID = grpGUID;
		
		this.componentReplies = new HashMap<Integer, LinkedList<String>>();
		//this.componentReplies = new HashMap<Integer, JSONArray>();
		//this.scheme = scheme;
		this.userReqID = userReqID;
		this.userIP = userIP;
		this.userPort = userPort;
		this.expiryTime = expiryTime;
		
		regionalReplies = new HashMap<Integer, OverlappingInfoClass>();
		regionalRepliesSize = new HashMap<Integer, Integer>();
		
		requestCompl = false;
		
		// query parsing
		queryComponents = QueryParser.parseQueryNew(query);
		// do second processing to get ProcessingQueryComponents
		processingQueryComponents = new HashMap<String, ProcessingQueryComponent>();
		initializeProcessingQueryComponents();
	}
	
	/**
	 * just the parsing constructor.
	 * @param searchQuery
	 */
	public QueryInfo(String searchQuery)
	{
		ContextServiceLogger.getLogger().fine("QueryInfo(String searchQuery) cons searchQuery "
				+searchQuery);
		this.searchQuery = searchQuery;
		// query parsing
		queryComponents = QueryParser.parseQueryNew(searchQuery);
		ContextServiceLogger.getLogger().fine("QueryInfo(String searchQuery) cons query parsing compl");
		// do second processing to get ProcessingQueryComponents
		processingQueryComponents = new HashMap<String, ProcessingQueryComponent>();
		initializeProcessingQueryComponents();
		ContextServiceLogger.getLogger().fine("QueryInfo(String searchQuery) cons initializing processing qc complete");
	}

	
	public String getQuery()
	{
		return searchQuery;
	}
	
	public String getGroupGUID()
	{
		return this.groupGUID;
	}
	
	public NodeIDType getSourceNodeId()
	{
		return sourceNodeId;
	}
	
	public long getRequestId()
	{
		return requestId;
	}
	
	public long getUserReqID()
	{
		return this.userReqID;
	}
	
	public String getUserIP()
	{
		return this.userIP;
	}
	
	public int getUserPort()
	{
		return this.userPort;
	}
	
	/*public void setQueryComponents(Vector<QueryComponent> qc)
	{
		queryComponents.addAll(qc);
	}*/
	
	public void setQueryRequestID(long requestId)
	{
		this.requestId = requestId;
	}
	
	public void setHyperdexResults(JSONArray hyperdexResults)
	{
		this.hyperdexResultArray = hyperdexResults;
	}
	
	public void setRequestCompl()
	{
		this.requestCompl = true;
	}
	
	public boolean getRequestCompl()
	{
		return this.requestCompl;
	}
	
	public JSONArray getHyperdexResults()
	{
		return this.hyperdexResultArray;
	}
	
	public long getExpiryTime()
	{
		return this.expiryTime;
	}
	
	/**
	 * Initialize regional replies with number of regions 
	 * contacted for the search query.
	 */
	public void initializeRegionalReplies(HashMap<Integer, OverlappingInfoClass> regionalReplies)
	{
		this.regionalReplies = regionalReplies;
	}
	
	public boolean setRegionalReply(Integer senderID, 
			QueryMesgToSubspaceRegionReply<NodeIDType> queryMesgToSubspaceRegionReply)
	{
		synchronized(this.regionalRepliesLock)
		{
			if(ContextServiceConfig.sendFullReplies)
			{
				OverlappingInfoClass overlapObj = this.regionalReplies.get(senderID);
				overlapObj.replyArray = queryMesgToSubspaceRegionReply.getResultGUIDs();
				this.regionalReplies.put(senderID, overlapObj);
			}
			else
			{
				this.regionalRepliesSize.put(senderID, queryMesgToSubspaceRegionReply.returnReplySize());
			}
			
			regionalRepliesCounter++;
			
			// replies from all regions revd.
			if(regionalRepliesCounter == this.regionalReplies.size())
			{
				return true;
			}
			else
			{
				return false;
			}
		}
	}
	
	public HashMap<Integer, OverlappingInfoClass> getRepliesHashMap()
	{
		return this.regionalReplies;
	}
	
	public HashMap<Integer, Integer> getRepliesSizeHashMap()
	{
		return this.regionalRepliesSize;
	}
	
	private void initializeProcessingQueryComponents()
	{
		for(int i=0;i<queryComponents.size();i++)
		{
			QueryComponent qcomponent = queryComponents.get(i);
			switch(qcomponent.getComponentType())
			{
				case QueryComponent.COMPARISON_PREDICATE:
				{
					String attrName = qcomponent.getAttributeName();
					String operator = qcomponent.getOperator();
					String value = qcomponent.getValue();
					
					ContextServiceLogger.getLogger().fine("attrName "+attrName+" oper "+operator+" val "+value);
					AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
					ProcessingQueryComponent pqc = processingQueryComponents.get(attrName);
					if(pqc == null)
					{
						pqc = new ProcessingQueryComponent
								(attrName, attrMetaInfo.getMinValue(), attrMetaInfo.getMaxValue());
						processingQueryComponents.put(attrName, pqc);
					}
					
					if( operator.equals("<=") )
					{				
						pqc.setUpperBound(value);
						ContextServiceLogger.getLogger().fine("<= case attrName "+attrName+" oper "+operator+" val "+pqc.getLowerBound() +" " + pqc.getUpperBound());
					}
					else if( operator.equals(">="))
					{
						pqc.setLowerBound(value);
						ContextServiceLogger.getLogger().fine(">= case attrName "+attrName+" oper "+operator+" val "+pqc.getLowerBound() +" " + pqc.getUpperBound());
					}
					else if(operator.equals("="))
					{
						pqc.setLowerBound(value);
						pqc.setUpperBound(value);
						ContextServiceLogger.getLogger().fine("= case attrName "+attrName+" oper "+operator+" val "+pqc.getLowerBound() +" " + pqc.getUpperBound());
					}
					
					break;
				}
				case QueryComponent.FUNCTION_PREDICATE:
				{
					Vector<ProcessingQueryComponent> pqcVect = qcomponent.getFunction().getProcessingQueryComponents();
					for(int j=0;j<pqcVect.size(); j++)
					{
						ProcessingQueryComponent pqc = pqcVect.get(j);
						processingQueryComponents.put(pqc.getAttributeName(), pqc);
					}
					break;
				}
				case QueryComponent.JOIN_INFO:
				{
					//FIXME: pending
					break;
				}
			}
		}
	}
	
	public Vector<QueryComponent> getQueryComponents()
	{
		return this.queryComponents;
	}
	
	public HashMap<String, ProcessingQueryComponent> getProcessingQC()
	{
		return this.processingQueryComponents;
	}
}