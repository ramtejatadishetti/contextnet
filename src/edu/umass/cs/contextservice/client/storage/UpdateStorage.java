package edu.umass.cs.contextservice.client.storage;

import edu.umass.cs.contextservice.client.callback.interfaces.CallBackInterface;
import edu.umass.cs.contextservice.client.callback.interfaces.UpdateReplyInterface;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNSReply;

public class UpdateStorage
{
	public long requestID;
	public ValueUpdateFromGNS valUpdFromGNS;
	public ValueUpdateFromGNSReply valUpdFromGNSReply;
	
	public UpdateReplyInterface updReplyObj;
	public CallBackInterface callback;
	//public boolean blocking;
}