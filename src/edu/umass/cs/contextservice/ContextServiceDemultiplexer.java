package edu.umass.cs.contextservice;

import org.json.JSONObject;

import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.nioutils.NIOInstrumenter;


/**
 * Just a dummy multiplexer
 * @author ayadav
 */
public class ContextServiceDemultiplexer extends AbstractJSONPacketDemultiplexer
{
	@Override
	public final boolean handleMessage(JSONObject jsonObject) 
	{
		NIOInstrumenter.incrPktsRcvd();
		return false; // must remain false;
	}
}