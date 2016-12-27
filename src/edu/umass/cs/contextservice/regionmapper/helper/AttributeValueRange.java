package edu.umass.cs.contextservice.regionmapper.helper;

public class AttributeValueRange
{
	// values are in String format. Based on the 
	// datatype of the attribute, the values should be type casted to 
	// the correct data type.
	private String lowerBound;
	private String upperBound;
	
	public AttributeValueRange(String lowerBound, String upperBound)
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
	
	public void setLowerBound(String lowerBound)
	{
		this.lowerBound = lowerBound;
	}
	
	public void setUpperBound(String upperBound)
	{
		this.upperBound = upperBound;
	}
}