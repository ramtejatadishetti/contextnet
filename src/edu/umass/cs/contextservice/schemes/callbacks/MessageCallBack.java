package edu.umass.cs.contextservice.schemes.callbacks;

import edu.umass.cs.contextservice.messages.BasicContextServicePacket;

public class MessageCallBack<NodeIDType>
{
	protected final BasicContextServicePacket<NodeIDType> csPacket;
	
	public MessageCallBack(BasicContextServicePacket<NodeIDType> csPacket)
	{
		this.csPacket = csPacket;
	}
	
}