package edu.umass.cs.contextservice.processing;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.messages.BulkGetReply;
import edu.umass.cs.contextservice.messages.QueryMsgToValuenode;

public class RecordReadStorage<NodeIDType>
{
	private final long recordStorageNum;
	
	// num of different nodes contacted == num of bulk get sent == 
	// num of replies to be received
	private final int numDiffNodesContacted;
	
	private final QueryMsgToValuenode<NodeIDType> queryMsgToValnode;
	
	private int numGetRepliesRecvd;
	
	// stores bulk get replies as and when they are recvd
	private JSONArray repliesArray;
	
	//private final Object repliesRecvdMonitor = new Object();
	private final Object repliesArrayMonitor = new Object();
	
	// user query
	/*private final String query;
	private final NodeIDType sourceNodeId;
	private final long requestId;
	private final String groupGUID;
	
	// req id set by the user
	private final long userReqID;
	private final String userIP;
	private final int userPort;
	
	// just for debugging and experimentation purpose
	private final long creationTime;
	
	//private final AbstractScheme<NodeIDType> scheme;
	// stores the parsed query components
	public Vector<QueryComponent> queryComponents;
	// stores the replies recvd from the value nodes for the query
	// Hash map indexed by componentId, and Vector<String> stores 
	// the GUIDs
	public HashMap<Integer, LinkedList<String>> componentReplies;
	
	private JSONArray hyperdexResultArray;
	
	// for synch
	private boolean requestCompl;*/
	
	public RecordReadStorage(long recordStorageNum, int numDiffNodesContacted, 
			QueryMsgToValuenode<NodeIDType> queryMsgToValnode)
	{
		this.recordStorageNum = recordStorageNum;
		
		this.numDiffNodesContacted = numDiffNodesContacted;
		
		this.queryMsgToValnode = queryMsgToValnode;
		
		numGetRepliesRecvd = 0;
		
		repliesArray = new JSONArray();
		
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
	
	public JSONArray addBulkGetReply( BulkGetReply<NodeIDType> bulkGetReplyMesg )
	{
		synchronized(repliesArrayMonitor)
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
			
			System.out.println("addBulkGetReply numGetRepliesRecvd "+numGetRepliesRecvd+
					" numDiffNodesContacted "+numDiffNodesContacted);
			
			if( numGetRepliesRecvd == numDiffNodesContacted )
			{
				return repliesArray;
			}
			else
			{
				return null;
			}
		}
	}
	
	public QueryMsgToValuenode<NodeIDType> getQueryMsgToValnode()
	{
		return this.queryMsgToValnode;
	}
}