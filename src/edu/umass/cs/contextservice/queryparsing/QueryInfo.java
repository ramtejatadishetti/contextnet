package edu.umass.cs.contextservice.queryparsing;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegionReply;
import edu.umass.cs.contextservice.schemes.helperclasses.RegionInfoClass;
import edu.umass.cs.contextservice.schemes.helperclasses.SubspaceSearchReplyInfo;

/**
 * Class to store the query related information, 
 * like query, its source etc
 * @author ayadav
 */
public class QueryInfo
{
	// if a query is a where or a join query
	public static final int WHERE_QUERY								= 1;
	public static final int JOIN_QUERY								= 2;
	
	// user query
	private final String searchQuery;
	private  Integer sourceNodeId;
	private  long requestId;
	private  String groupGUID;
	
	// req id set by the user
	private  long userReqID;
	private  String userIP;
	private  int userPort;
	
	private long expiryTime;
	
	
	// stores the parsed query components
	private Vector<QueryComponent> queryComponents;
	
	// indexed by attrName
	private HashMap<String, ProcessingQueryComponent> processingQueryComponents;
	
	// for synch
	private boolean requestCompl;
	
	// for hyperspace privacy and no privacy case.
	// to store replies of each region of subspace
	//private HashMap<Integer, OverlappingInfoClass> regionalReplies;
	//private HashMap<Integer, Integer> regionalRepliesSize;
	
	// For no privacy and hyperspace privacy this map has only
	// one entry, as in these schemes a search query only goes to one 
	// subspace. For subsapce privacy scheme this map can have multiple 
	// entries.
	// key is subspace ID, 
	private HashMap<Integer, SubspaceSearchReplyInfo> searchReplyMap;
	
	private final Object addReplyLock = new Object();
	
	public QueryInfo( String query, 
			Integer sourceNodeId, String grpGUID, long userReqID, 
			String userIP, int userPort, long expiryTime )
	{
		this.searchQuery = query;
		this.sourceNodeId = sourceNodeId;
		this.groupGUID = grpGUID;
		
		this.userReqID = userReqID;
		this.userIP = userIP;
		this.userPort = userPort;
		this.expiryTime = expiryTime;
		
		searchReplyMap = new HashMap<Integer, SubspaceSearchReplyInfo>();
		
		
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
		ContextServiceLogger.getLogger().fine("QueryInfo(String searchQuery) "
				+ "cons initializing processing qc complete");	
	}
	
	
	public String getQuery()
	{
		return searchQuery;
	}
	
	public String getGroupGUID()
	{
		return this.groupGUID;
	}
	
	public Integer getSourceNodeId()
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
	
	public void setQueryRequestID(long requestId)
	{
		this.requestId = requestId;
	}
	
	public void setRequestCompl()
	{
		this.requestCompl = true;
	}
	
	public boolean getRequestCompl()
	{
		return this.requestCompl;
	}
	
	public long getExpiryTime()
	{
		return this.expiryTime;
	}
	
	public HashMap<Integer, SubspaceSearchReplyInfo> getSearchReplyMap()
	{
		return this.searchReplyMap;
	}
	
	/**
	 * Initialize regional replies with number of regions 
	 * contacted for the search query.
	 */
	public void initializeSearchQueryReplyInfo
						(HashMap<Integer, SubspaceSearchReplyInfo> searchQueryReplyInfo)
	{
		this.searchReplyMap = searchQueryReplyInfo;
	}
	
	public boolean addReplyFromARegionOfASubspace(int subspaceId, int senderID, 
			QueryMesgToSubspaceRegionReply queryMesgToSubspaceRegionReply)
	{
		synchronized(this.addReplyLock)
		{
			SubspaceSearchReplyInfo subspaceSearchReply = searchReplyMap.get(subspaceId);
			
			RegionInfoClass regionInfoClass = subspaceSearchReply.overlappingRegionsMap.get(senderID);
			
			if( ContextServiceConfig.sendFullRepliesWithinCS )
			{
				regionInfoClass.replyArray = queryMesgToSubspaceRegionReply.getResultGUIDs();
				regionInfoClass.numReplies = queryMesgToSubspaceRegionReply.returnReplySize();
			}
			else
			{
				regionInfoClass.numReplies = queryMesgToSubspaceRegionReply.returnReplySize();
			}
			
			subspaceSearchReply.regionRepliesCounter++;
			
			
			if( checkForRequestCompletion() )
			{
				return true;
			}
			else
			{
				return false;
			}
		}
	}
	
	/**
	 * This method requires synchronzied execution.
	 * Or atleast it is assumed right now.
	 * @return
	 */
	private boolean checkForRequestCompletion()
	{
		boolean completion = true;
		Iterator<Integer> subspaceIter = searchReplyMap.keySet().iterator();
		
		while( subspaceIter.hasNext() )
		{
			int subsapceId = subspaceIter.next();
			SubspaceSearchReplyInfo subspaceSearchInfo = searchReplyMap.get(subsapceId);
			
			if(subspaceSearchInfo.regionRepliesCounter != subspaceSearchInfo.overlappingRegionsMap.size())
			{
				completion = false;
				break;
			}
		}
		return completion;
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