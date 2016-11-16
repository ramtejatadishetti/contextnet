package edu.umass.cs.contextservice.schemes.callbacks;

import edu.umass.cs.contextservice.messages.BasicContextServicePacket;

public class MessageCallBack
{
	protected final BasicContextServicePacket csPacket;
	
	public MessageCallBack(BasicContextServicePacket csPacket)
	{
		this.csPacket = csPacket;
	}
	
}