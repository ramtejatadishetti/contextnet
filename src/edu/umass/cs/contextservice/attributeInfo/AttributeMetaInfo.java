package edu.umass.cs.contextservice.attributeInfo;

import java.util.Random;

public class AttributeMetaInfo
{
	private final String attributeName;
	
	// irrespective of attribute datatype
	// min and max values ae stored in string type
	// and are converted to the required type on fly
	private final String minValue;
	private final String maxValue;
	
	// used as default value in hyperspace if none is specified.
	public final String defaultValue;
	
	private final String dataType;
	
	public AttributeMetaInfo( String attributeName, String minValue, 
			String maxValue, String dataType )
	{
		this.attributeName = attributeName;
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.defaultValue = minValue;
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
	
	public String getDataType()
	{
		return this.dataType;
	}
	
	public String getDefaultValue()
	{
		return this.defaultValue;
	}
	
	public String getARandomValue(Random randGenerator)
	{
		String randVal = "";
		
		switch( dataType )
		{
			case AttributeTypes.IntType:
			{
				int minValueInt = Integer.parseInt(minValue);
				int maxValueInt = Integer.parseInt(maxValue);
				int randValInt 
						= (int)(randGenerator.nextDouble() * (maxValueInt-minValueInt));
				randVal = randValInt+"";
				break;
			}
			case AttributeTypes.LongType:
			{
				long minValueLong = Long.parseLong(minValue);
				long maxValueLong = Long.parseLong(maxValue);
				long randValLong  
						= (long)(randGenerator.nextDouble() * (maxValueLong-minValueLong));
				randVal = randValLong+"";
				break;
			}
			case AttributeTypes.DoubleType:
			{
				double minValueDouble = Double.parseDouble(minValue);
				double maxValueDouble = Double.parseDouble(maxValue);
				double randValDouble  
						= (double)(randGenerator.nextDouble() * (maxValueDouble-minValueDouble));
				randVal = randValDouble+"";
				break;
			}
			case AttributeTypes.StringType:
			{
				//FIXME: not supported yet, will need to do that
				assert(false);
				//break;
			}
			default:
				assert(false);
		}
		
		assert(randVal.length() > 0);
		return randVal;	
	}
}