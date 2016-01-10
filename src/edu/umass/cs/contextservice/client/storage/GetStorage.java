package edu.umass.cs.contextservice.client.storage;

import edu.umass.cs.contextservice.messages.GetMessage;
import edu.umass.cs.contextservice.messages.GetReplyMessage;

public class GetStorage<NodeIDType>
{
	public long requestID;
	public GetMessage<NodeIDType> getMessage;
	public GetReplyMessage<NodeIDType> getReplyMessage;
}