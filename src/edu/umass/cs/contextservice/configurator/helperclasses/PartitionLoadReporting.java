package edu.umass.cs.contextservice.configurator.helperclasses;

public class PartitionLoadReporting
{
	private final PartitionToNodeInfo partitionNodeInf;
	// load in requests/s
	private final double partitionLoad;
	
	public PartitionLoadReporting( PartitionToNodeInfo partitionNodeInf, 
			double partitionLoad )
	{
		this.partitionNodeInf = partitionNodeInf;
		this.partitionLoad = partitionLoad;
	}
	
	public PartitionToNodeInfo getParititionInfo()
	{
		return partitionNodeInf;
	}
	
	public double getLoad()
	{
		return partitionLoad;
	}
}