package edu.umass.cs.contextservice.networktransmission.packets;

import java.io.UnsupportedEncodingException;

public class ClientConfigRequest extends AbstractContextServicePacket
{
	public ClientConfigRequest(int packetType) 
	{
		super(packetType);
		
		
	}

	@Override
	public byte[] getBytes() throws UnsupportedEncodingException 
	{
		
		return null;
	}
}