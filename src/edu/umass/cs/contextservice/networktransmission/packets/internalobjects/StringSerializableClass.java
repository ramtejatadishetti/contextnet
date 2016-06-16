package edu.umass.cs.contextservice.networktransmission.packets.internalobjects;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import edu.umass.cs.contextservice.config.ContextServiceConfig;

/**
 * This class serializes a string into a byte[]
 * @author adipc
 */
public class StringSerializableClass
{
	private final String str;
	
	public StringSerializableClass(String str)
	{
		this.str = str;
	}
	
	public byte[] getBytes() throws UnsupportedEncodingException
	{
		byte[] strByteArray = str.getBytes(ContextServiceConfig.STRING_ENCODING);
		
		int sizeOFByteBuffer = Integer.BYTES + strByteArray.length;
		ByteBuffer buf = ByteBuffer.allocate(sizeOFByteBuffer);
		buf.putInt(strByteArray.length);
		buf.put(strByteArray);
		buf.flip();
		return buf.array();
	}
}