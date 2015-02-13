package edu.umass.cs.contextservice;

import org.json.JSONObject;

import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;

/**
 * Just a dummy multiplexer
 * @author ayadav
 *
 */
public class ContextServiceDemultiplexer extends AbstractPacketDemultiplexer 
{
	@Override
	public boolean handleJSONObject(JSONObject jsonObject) 
	{
		incrPktsRcvd();
	    return false; // WARNING: Do not change this to true. It could break the GNS by not trying any other PDs.
	}
}