package edu.umass.cs.contextservice.client.storage;

import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNSReply;

public class UpdateStorage<NodeIDType>
{
	public long requestID;
	public ValueUpdateFromGNS<NodeIDType> valUpdFromGNS;
	public ValueUpdateFromGNSReply<NodeIDType> valUpdFromGNSReply;
}