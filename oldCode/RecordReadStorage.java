package edu.umass.cs.contextservice.queryparsing;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.BulkGetReply;
import edu.umass.cs.contextservice.messages.QueryMsgToValuenode;

public class RecordReadStorage<Integer>
{
	private final long recordStorageNum;
	
	// num of different nodes contacted == num of bulk get sent == 
	// num of replies to be received
	private final int numDiffNodesContacted;
	
	private final QueryMsgToValuenode<Integer> queryMsgToValnode;
	
	private int numGetRepliesRecvd;
	
	private ConcurrentHashMap<Integer, JSONArray> repliesMap;
	// stores bulk get replies as and when they are recvd
	//private JSONArray repliesArray;
	
	//private final Object repliesRecvdMonitor = new Object();
	private final Object repliesArrayMonitor = new Object();
	
	public RecordReadStorage(long recordStorageNum, int numDiffNodesContacted, 
			QueryMsgToValuenode<Integer> queryMsgToValnode)
	{
		this.recordStorageNum = recordStorageNum;
		
		this.numDiffNodesContacted = numDiffNodesContacted;
		
		this.queryMsgToValnode = queryMsgToValnode;
		
		numGetRepliesRecvd = 0;
		
		//repliesArray = new JSONArray();
		
		repliesMap = new ConcurrentHashMap<Integer, JSONArray>();
		
		/*this.query = query;
		this.sourceNodeId = sourceNodeId;
		this.requestId = requestID;
		this.groupGUID = grpGUID;
		this.queryComponents = new Vector<QueryComponent>();
		this.componentReplies = new HashMap<Integer, LinkedList<String>>();
		//this.scheme = scheme;
		this.userReqID = userReqID;
		this.userIP = userIP;
		this.userPort = userPort;	
		this.creationTime = System.currentTimeMillis();
		requestCompl = false;*/
	}
	
	public JSONArray addBulkGetReply( BulkGetReply<Integer> bulkGetReplyMesg )
	{
		JSONArray apppendArray = bulkGetReplyMesg.getGUIDRecords();
		if(apppendArray != null)
		{
			int myRepNum = 0;
			synchronized(repliesArrayMonitor)
			{
				numGetRepliesRecvd++;
				myRepNum = numGetRepliesRecvd;
			}
			this.repliesMap.put(myRepNum, apppendArray);
			
			ContextServiceLogger.getLogger().fine("addBulkGetReply numGetRepliesRecvd "+numGetRepliesRecvd+
					" numDiffNodesContacted "+numDiffNodesContacted);
			
			// used myRepNum here instead of numGetRepliesRecvd for synchronization
			if( myRepNum == numDiffNodesContacted )
			{
				//return repliesArray;
				Iterator<Integer> mapKeys = this.repliesMap.keySet().iterator();
				JSONArray answerJSON = new JSONArray();
				while(mapKeys.hasNext())
				{
					JSONArray currJSON = this.repliesMap.get(mapKeys.next());
					for(int i=0;i<currJSON.length();i++)
					{
						try 
						{
							answerJSON.put(currJSON.getString(i));
						} catch (JSONException e) 
						{
							e.printStackTrace();
						}
					}
				}
				return answerJSON;
			}
			else
			{
				return null;
			}
		}
		else
		{
			synchronized(repliesArrayMonitor)
			{
				numGetRepliesRecvd++;
			}
			return null;
		}
		
		/*synchronized(repliesArrayMonitor)
		{
			JSONArray apppendArray = bulkGetReplyMesg.getGUIDRecords();
			
			if(apppendArray != null)
			{
				for(int i=0; i<apppendArray.length(); i++)
				{
					try
					{
						this.repliesArray.put( apppendArray.getString(i) );
					} catch (JSONException e)
					{
						e.printStackTrace();
					}
				}
			}
			numGetRepliesRecvd++;
			
			ContextServiceLogger.getLogger().fine("addBulkGetReply numGetRepliesRecvd "+numGetRepliesRecvd+
					" numDiffNodesContacted "+numDiffNodesContacted);
			
			if( numGetRepliesRecvd == numDiffNodesContacted )
			{
				return repliesArray;
			}
			else
			{
				return null;
			}
		}*/
	}
	
	public QueryMsgToValuenode<Integer> getQueryMsgToValnode()
	{
		return this.queryMsgToValnode;
	}
}