package edu.umass.cs.contextservice.database.records;

public class MetadataTableInfo<Integer>
{
	private final Integer nodeID;
	private final int partitionNum;
	
	public MetadataTableInfo(Integer nodeID, int partitionNum)
	{
		this.nodeID = nodeID;
		this.partitionNum = partitionNum;
	}
	
	public Integer getNodeID()
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