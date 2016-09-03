package edu.umass.cs.contextservice.common;

import org.json.JSONObject;

import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.nioutils.NIOHeader;


/**
 * Just a dummy multiplexer
 * @author ayadav
 */
public class ContextServiceDemultiplexer extends AbstractJSONPacketDemultiplexer
{
	@Override
	public final boolean handleMessage(JSONObject jsonObject, NIOHeader nioHeader) 
	{
		return false; // must remain false;
	}
}