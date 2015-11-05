package edu.umass.cs.contextservice.processing;


import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;

public class UpdateInfo<NodeIDType>
{
	private final ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS;
	private final long updateRequestId;
	
	// just for debugging the large update times.
	//private final long updateStartTime;
	//private final long contextStartTime;
	
	// stores the replies recvd from the value nodes for the query
	// Hash map indexed by componentId, and Vector<String> stores 
	// the GUIDs
	//public final HashMap<Integer, LinkedList<String>> repliesMap;
	int numReplyRecvd;
	
	private boolean updateReqCompl;
	
	public UpdateInfo(ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS, long updateRequestId)
	{
		this.valUpdMsgFromGNS = valUpdMsgFromGNS;
		this.updateRequestId = updateRequestId;
		numReplyRecvd = 0;
		//contextStartTime = System.currentTimeMillis();
		//updateStartTime = valUpdMsgFromGNS.getUpdateStartTime();
		
		updateReqCompl = false;
	}
	
	public long getRequestId()
	{
		return updateRequestId;
	}
	
	public synchronized void incrementNumReplyRecvd()
	{
		this.numReplyRecvd++;
	}
	
	public int getNumReplyRecvd()
	{
		return this.numReplyRecvd;
	}
	
	public ValueUpdateFromGNS<NodeIDType> getValueUpdateFromGNS()
	{
		return this.valUpdMsgFromGNS;
	}
	
	/*public long getUpdateStartTime()
	{
		return this.updateStartTime;
	}*/
	
	/*public long getContextStartTime()
	{
		return this.contextStartTime;
	}*/
	
	public boolean getUpdComl()
	{
		return this.updateReqCompl;
	}
	
	public void  setUpdCompl()
	{
		this.updateReqCompl = true;
	}
}