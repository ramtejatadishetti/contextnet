package edu.umass.cs.contextservice.networktransmission.packets;

import java.nio.ByteBuffer;

public class QueryMesgFromUserReplyPacket extends AbstractContextServicePacket
{
	private final long userReqNum;

	// this is the number of anonymized IDs or GUIDs in reply.
	private final int resultSize;
	
	// this is the total length in bytes.
	private final int totalResultByteArrayLength;
	// this byte would still need serialization
	private final byte[] resultByteArray;
	
	
	public QueryMesgFromUserReplyPacket( int packetType, long userReqNum, 
			int resultSize, int totalResultByteArrayLength, byte[] resultByteArray )
	{
		super(packetType);
		this.userReqNum = userReqNum;
		this.resultSize = resultSize;
		this.totalResultByteArrayLength = totalResultByteArrayLength;
		this.resultByteArray = resultByteArray;
	}
	
	@Override
	public byte[] getBytes()
	{
		int sizeOFByteBuffer = Long.BYTES + 2*Integer.BYTES + totalResultByteArrayLength;
		ByteBuffer buf = ByteBuffer.allocate(sizeOFByteBuffer);
		buf.putLong(userReqNum);
		buf.putInt(resultSize);
		buf.putInt(totalResultByteArrayLength);
		buf.put(resultByteArray);
		buf.flip();
		return buf.array();
	}
}