package edu.umass.cs.contextservice.schemes.callbacks;

import edu.umass.cs.contextservice.messages.BasicContextServicePacket;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;

public class QueryMessageFromUserCallBack 
											extends MessageCallBack
{
	private final QueryInfo currReq;
	
	public QueryMessageFromUserCallBack( BasicContextServicePacket csPacket,
			QueryInfo currReq )
	{
		super(csPacket);
		this.currReq = currReq;
	}
	
	
	
}