package edu.umass.cs.contextservice.configurator.helperclasses;

import java.util.HashMap;

public class PartitionToNodeInfo
{
	private final int subspaceId;
	private final int replicaNum;
	private final HashMap<String, RangeInfo> attrBound;
	private final Integer respNodeId;
	
	
	public PartitionToNodeInfo( int subspaceId, int replicaNum, 
			HashMap<String, RangeInfo> attrBound, Integer respNodeId )
	{
		this.subspaceId = subspaceId;
		this.replicaNum = replicaNum;
		this.attrBound = attrBound;
		this.respNodeId = respNodeId;
	}
	
	public int getSubspaceId()
	{
		return this.subspaceId;
	}
	
	public int replicaNum()
	{
		return this.replicaNum;
	}
	
	public HashMap<String, RangeInfo> getAttrBounds()
	{
		return attrBound;
	}
	
	public Integer getRespNodeId()
	{
		return this.respNodeId;
	}
}