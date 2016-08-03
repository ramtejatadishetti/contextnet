package edu.umass.cs.contextservice.schemes.callbacks;

import edu.umass.cs.contextservice.messages.BasicContextServicePacket;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;

public class QueryMessageFromUserCallBack<NodeIDType> 
											extends MessageCallBack<NodeIDType>
{
	private final QueryInfo<NodeIDType> currReq;
	
	public QueryMessageFromUserCallBack( BasicContextServicePacket<NodeIDType> csPacket,
			QueryInfo<NodeIDType> currReq )
	{
		super(csPacket);
		this.currReq = currReq;
	}
	
	
	
}