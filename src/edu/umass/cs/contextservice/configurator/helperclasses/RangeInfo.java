package edu.umass.cs.contextservice.configurator.helperclasses;

public class RangeInfo
{
	private String lowerBound;
	private String upperBound;
	
	
	public RangeInfo(String lowerBound, String upperBound)
	{
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
	}
	
	public String getLowerBound()
	{
		return this.lowerBound;
	}
	
	public String getUpperBound()
	{
		return this.upperBound;
	}
}