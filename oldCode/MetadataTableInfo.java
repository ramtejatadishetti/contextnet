package edu.umass.cs.contextservice.database.records;

public class MetadataTableInfo<NodeIDType>
{
	private final NodeIDType nodeID;
	private final int partitionNum;
	
	public MetadataTableInfo(NodeIDType nodeID, int partitionNum)
	{
		this.nodeID = nodeID;
		this.partitionNum = partitionNum;
	}
	
	public NodeIDType getNodeID()
	{
		return this.nodeID;
	}
	
	public int getPartitionNum()
	{
		return this.partitionNum;
	}
	
	public String toString()
	{
		return "this.nodeID "+this.nodeID+" this.partitionNum "+this.partitionNum;
	}
}