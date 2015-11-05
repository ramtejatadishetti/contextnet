package edu.umass.cs.contextservice.hyperspace.storage;

public class DomainPartitionInfo
{
	private final int partitionNum;
	private final double lowerbound;
	private final double upperbound;
	
	public DomainPartitionInfo(int partitionNum, double lowerbound, double upperbound)
	{
		this.partitionNum = partitionNum;
		this.lowerbound = lowerbound;
		this.upperbound = upperbound;
	}
	
	public int getPartitionNum()
	{
		return this.partitionNum;
	}
	
	public double getLowerbound()
	{
		return this.lowerbound;
	}
	
	public double getUpperbound()
	{
		return this.upperbound;
	}
}