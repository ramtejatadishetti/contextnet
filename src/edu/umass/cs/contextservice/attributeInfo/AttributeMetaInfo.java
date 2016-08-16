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
	
	
	// true if the default value is lower than the minimum 
	// value. False if the default value is higher than the upper value.
	// default value can never be in between the minimum and maximum value.
	private boolean isLowerValDefault;
	
	public AttributeMetaInfo( String attributeName, String minValue, 
			String maxValue, String dataType )
	{
		this.attributeName = attributeName;
		this.minValue = minValue;
		this.maxValue = maxValue;
		// FIXME: just for the experiments.
		// need to take default values as inputs as we used to.
		this.defaultValue = 0+"";
		this.isLowerValDefault = true;
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
	
	public boolean isLowerValDefault()
	{
		return this.isLowerValDefault;
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
						= minValueInt + (int)(randGenerator.nextDouble() * (maxValueInt-minValueInt));
				
				assert(randValInt >= minValueInt);
				assert(randValInt < maxValueInt);
				
				randVal = randValInt+"";
				break;
			}
			case AttributeTypes.LongType:
			{
				long minValueLong = Long.parseLong(minValue);
				long maxValueLong = Long.parseLong(maxValue);
				long randValLong  
						= minValueLong + (long)(randGenerator.nextDouble() * (maxValueLong-minValueLong));
				
				assert(randValLong >= minValueLong);
				assert(randValLong < maxValueLong);
				
				randVal = randValLong+"";
				break;
			}
			case AttributeTypes.DoubleType:
			{
				double minValueDouble = Double.parseDouble(minValue);
				double maxValueDouble = Double.parseDouble(maxValue);
				double randValDouble   
					= minValueDouble + 
					(double)(randGenerator.nextDouble() * (maxValueDouble-minValueDouble));
				
				assert(randValDouble >= minValueDouble);
				assert(randValDouble < maxValueDouble);
				
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