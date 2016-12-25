package edu.umass.cs.contextservice.regionmapper.helper;

public class AttributeValueRange
{
	// values are in String format. Based on the 
	// datatype of the attribute, the values should be type casted to 
	// the correct data type.
	private final String lowerBound;
	private final String upperBound;
	
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
}