package edu.umass.cs.contextservice.regionmapper.helper;

import java.util.HashMap;
import java.util.Iterator;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;

/**
 * This class stores a value space and methods to edit that value space.
 * Like in space partitioning, this class initially contains whole value space 
 * and then as the space gets partitioned into regions, the value space stored in this
 * class can be edited to the remaining unparititioned value space.
 * 
 * @author ayadav
 */
public class ValueSpaceInfo
{
	private final HashMap<String, AttributeValueRange> valueSpaceBoundary;
	
	public ValueSpaceInfo()
	{
		valueSpaceBoundary = new HashMap<String, AttributeValueRange>();
	}
	
	public HashMap<String, AttributeValueRange> getValueSpaceBoundary()
	{
		return this.valueSpaceBoundary;
	}
	
//	public void setValueSpaceBoundary(HashMap<String, AttributeValueRange> valSpaceBoundary)
//	{
//		this.valueSpaceBoundary = valSpaceBoundary;
//	}
	
	public String toString()
	{
		String str = "";
		Iterator<String> attrIter = valueSpaceBoundary.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			AttributeValueRange attrValRange = valueSpaceBoundary.get(attrName);
			
			str = str +attrName+","+attrValRange.getLowerBound()+","
							+attrValRange.getUpperBound();
			
			if(attrIter.hasNext())
				str= str+",";
		}
		return str;
	}
	
	public static ValueSpaceInfo fromString(String str)
	{
		ValueSpaceInfo newValSpace 	= new ValueSpaceInfo();
		
		HashMap<String, AttributeValueRange> valSpaceBoundary 
									= newValSpace.getValueSpaceBoundary();
		
		String[] parsed = str.split(",");
		
		int currPos = 0;
		
		while(currPos < parsed.length)
		{
			String attrName = parsed[currPos];
			currPos++;
			
			String lowerBound = parsed[currPos];
			currPos++;
			
			String upperBound = parsed[currPos];
			currPos++;
			
			
			AttributeValueRange attrValRange = new AttributeValueRange(lowerBound, upperBound);
			valSpaceBoundary.put(attrName, attrValRange);
		}	
		return newValSpace;
	}
	
	public static ValueSpaceInfo getAllAttrsValueSpaceInfo(
			HashMap<String, AttributeValueRange> partialAttrValRange, 
			HashMap<String, AttributeMetaInfo> attributeMap)
	{
		ValueSpaceInfo valSpaceInfo = new ValueSpaceInfo();
		
		Iterator<String> attrIter = attributeMap.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			AttributeMetaInfo attrInfo = attributeMap.get(attrName);
			
			if( partialAttrValRange.containsKey(attrName) )
			{
				valSpaceInfo.getValueSpaceBoundary().put(attrName, 
							partialAttrValRange.get(attrName));
			}
			else
			{
				valSpaceInfo.getValueSpaceBoundary().put
					(attrName, new AttributeValueRange(attrInfo.getMinValue(), 
						attrInfo.getMaxValue()));
			}
		}
		
		assert(valSpaceInfo.getValueSpaceBoundary().size() == attributeMap.size());
		return valSpaceInfo;
	}
	
	
	public static boolean checkOverlapOfTwoValueSpaces( HashMap<String, AttributeMetaInfo> attributeMap, 
			ValueSpaceInfo valSpace1, ValueSpaceInfo valSpace2 )
	{
		assert(valSpace1.getValueSpaceBoundary().size() 
						<= attributeMap.size() );
		
		assert(valSpace2.getValueSpaceBoundary().size() 
				<= attributeMap.size() );
		
		assert(valSpace1.getValueSpaceBoundary().size() 
						== valSpace2.getValueSpaceBoundary().size());
		
		
		boolean overlap = true;
		
		Iterator<String> attrIter = valSpace1.getValueSpaceBoundary().keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			
			AttributeMetaInfo attrMetaInfo = attributeMap.get(attrName);
			
			AttributeValueRange attrValRange1  
								= valSpace1.getValueSpaceBoundary().get(attrName);
			
			AttributeValueRange attrValRange2 
								= valSpace2.getValueSpaceBoundary().get(attrName);
			
			assert(attrValRange2 != null);
			
			overlap = overlap && AttributeTypes.checkOverlapOfTwoIntervals(attrValRange1, 
					attrValRange2, attrMetaInfo.getDataType());
			
			if( !overlap )
			{
				break;
			}
		}
		return overlap;
	}
}