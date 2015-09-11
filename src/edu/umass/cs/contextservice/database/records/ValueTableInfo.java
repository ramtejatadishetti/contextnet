package edu.umass.cs.contextservice.database.records;

public class ValueTableInfo 
{
	public static enum Operations {REMOVE, UPDATE};
	
	private final double value;
	private final String nodeGUID;
	
	public ValueTableInfo(double value, String nodeGUID)
	{
		this.value = value;
		this.nodeGUID = nodeGUID;
	}
	
	public double getValue()
	{
		return this.value;
	}
	
	public String getNodeGUID()
	{
		return this.nodeGUID;
	}
}