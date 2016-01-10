package edu.umass.cs.contextservice.attributeInfo;


public class AttributeMetaInfo
{
	private final String attributeName;
	
	// irrespective of attibute datatype
	// min and max values ae stored in string type
	// and are converted to the required type on fly
	private final String minValue;
	private final String maxValue;
	
	// used as default value in hyperspace if none is specified.
	public final String defaultValue;
	
	private final String dataType;
	
	public AttributeMetaInfo(String attributeName, String minValue, String maxValue, String defaultValue, 
			String dataType)
	{
		this.attributeName = attributeName;
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.defaultValue = defaultValue;
		this.dataType = dataType;
	}
	
	public String getAttrName()
	{
		return this.attributeName;
	}
	
	public String getMinValue()
	{
		return this.minValue;
	}
	
	public String getMaxValue()
	{
		return this.maxValue;
	}
	
	public String getDefaultValue()
	{
		return this.defaultValue;
	}
	
	public String getDataType()
	{
		return this.dataType;
	}
}