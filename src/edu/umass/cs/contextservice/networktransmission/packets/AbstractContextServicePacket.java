package edu.umass.cs.contextservice.networktransmission.packets;

import java.io.UnsupportedEncodingException;

/**
 * Abstract context service packet class. 
 * Every context service packet extends this class. 
 * @author adipc
 *
 */
public abstract class AbstractContextServicePacket 
{
	protected final int packetType;
	public AbstractContextServicePacket(int packetType)
	{
		this.packetType = packetType;
	}
	
	
	public abstract byte[] getBytes() throws UnsupportedEncodingException;
	
}