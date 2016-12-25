package edu.umass.cs.contextservice.attributeInfo;

import java.util.Random;

import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;

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
	
	// range size, which is typically maxValue-minValue for Int, Long and Double data type.
	// FIXME: not sure what this parameter's value will be in String data type.
	private final double rangeSize;
	
	public AttributeMetaInfo( String attributeName, String minValue, 
			String maxValue, String dataType )
	{
		this.attributeName = attributeName;
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.dataType = dataType;
		
		this.defaultValue = getDataTypeMinimumValue();
		this.isLowerValDefault = true;
		
		rangeSize = computeRangeSize(minValue, maxValue);
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
	
	public double getRangeSize()
	{
		return this.rangeSize;
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
	
	
	/**
	 * Assigns range size based on the data type.
	 */
	public double computeRangeSize( String lowerBound, String upperBound )
	{
		switch( dataType )
		{
			case AttributeTypes.IntType:
			{
				int lowerValueInt = Integer.parseInt(lowerBound);
				int upperValueInt = Integer.parseInt(upperBound);
				
				return (double) (upperValueInt-lowerValueInt);
			}
			case AttributeTypes.LongType:
			{
				long lowerValueLong = Long.parseLong(lowerBound);
				long upperValueLong = Long.parseLong(upperBound);
				
				
				return (double) (upperValueLong-lowerValueLong);
			}
			case AttributeTypes.DoubleType:
			{
				double lowerValueDouble = Double.parseDouble(lowerBound);
				double upperValueDouble = Double.parseDouble(upperBound);
				
				
				return (double) (upperValueDouble-lowerValueDouble);
			}
			case AttributeTypes.StringType:
			{
				assert(false);
				break;
			}
			default:
				assert(false);
		}
		return -1;
	}
	
	
	public double computeIntervalToRangeRatio(AttributeValueRange attrValRange)
	{
		double intervalRangeRatio = 0.0;
		
		switch( dataType )
		{
			case AttributeTypes.IntType:
			case AttributeTypes.LongType:
			case AttributeTypes.DoubleType:
			{
				double lowerValueDouble = Double.parseDouble(attrValRange.getLowerBound());
				double upperValueDouble = Double.parseDouble(attrValRange.getUpperBound());
				intervalRangeRatio = (upperValueDouble-lowerValueDouble)/rangeSize;
				
				return intervalRangeRatio;
			}
			case AttributeTypes.StringType:
			{
				assert(false);
				break;
			}
			default:
				assert(false);
		}
		return -1;
	}
	
	
	private String getDataTypeMinimumValue()
	{
		String dataTypeMinVal = "";
		
		switch( dataType )
		{
			case AttributeTypes.IntType:
			{
				dataTypeMinVal = Integer.MIN_VALUE+"";
				break;
			}
			case AttributeTypes.LongType:
			{
				dataTypeMinVal = Long.MIN_VALUE +"";
				break;
			}
			case AttributeTypes.DoubleType:
			{
				// Double.MIN_VALUE is positive, so blogs say this is the min value of double.
				dataTypeMinVal = -Double.MAX_VALUE+"";
				break;
			}
			case AttributeTypes.StringType:
			{
				//TODO: String datatype is not very well tested.
				dataTypeMinVal = "";
				break;
			}
			default:
				assert(false);
		}
		return dataTypeMinVal;
	}
	
	
}