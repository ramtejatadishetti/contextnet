package edu.umass.cs.contextservice.networktransmission.packets;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

import edu.umass.cs.contextservice.networktransmission.packets.internalobjects.StringSerializableClass;

public class ClientConfigReplyPacket extends AbstractContextServicePacket
{
	private final List<InetSocketAddress> nodeSocketAddresses;
	private final List<String> attrArray;
	
	public ClientConfigReplyPacket( int packetType, 
			List<InetSocketAddress> nodeSocketAddresses, List<String> attrArray )
	{
		super(packetType);
		this.nodeSocketAddresses = nodeSocketAddresses;
		this.attrArray = attrArray;
	}
	
	@Override
	public byte[] getBytes() throws UnsupportedEncodingException
	{
		int sizeOFByteBuffer = Integer.BYTES + 2*Integer.BYTES*nodeSocketAddresses.size();
		ByteBuffer buf = ByteBuffer.allocate(sizeOFByteBuffer);
		buf.putInt(nodeSocketAddresses.size());
		
		for( int i = 0; i<nodeSocketAddresses.size(); i++ )
		{
			InetAddress ipAddr = nodeSocketAddresses.get(i).getAddress();
			int port = nodeSocketAddresses.get(i).getPort();
			buf.put(ipAddr.getAddress());
			buf.putInt(port);
		}
		
		buf.putInt(attrArray.size());
		
		for( int i=0; i < attrArray.size(); i++ )
		{
			StringSerializableClass stringSer 
						= new StringSerializableClass(attrArray.get(i));
			byte[] strBytes = stringSer.getBytes();
			buf.put(strBytes);
		}
		
		buf.flip();
		return buf.array();
	}
}