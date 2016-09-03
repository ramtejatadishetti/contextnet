package edu.umass.cs.contextservice.configurator.helperclasses;

public class PartitionLoadReporting<NodeIDType>
{
	private final PartitionToNodeInfo<NodeIDType> partitionNodeInf;
	// load in requests/s
	private final double partitionLoad;
	
	public PartitionLoadReporting( PartitionToNodeInfo<NodeIDType> partitionNodeInf, 
			double partitionLoad )
	{
		this.partitionNodeInf = partitionNodeInf;
		this.partitionLoad = partitionLoad;
	}
	
	public PartitionToNodeInfo<NodeIDType> getParititionInfo()
	{
		return partitionNodeInf;
	}
	
	public double getLoad()
	{
		return partitionLoad;
	}
}