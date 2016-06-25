package edu.umass.cs.contextservice.common;

import org.json.JSONObject;

import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;


/**
 * Just a dummy multiplexer
 * @author ayadav
 */
public class ContextServiceDemultiplexer extends AbstractJSONPacketDemultiplexer
{
	@Override
	public final boolean handleMessage(JSONObject jsonObject) 
	{
		return false; // must remain false;
	}
}