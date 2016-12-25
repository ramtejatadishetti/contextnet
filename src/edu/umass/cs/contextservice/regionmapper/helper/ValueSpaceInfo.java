package edu.umass.cs.contextservice.regionmapper.helper;

import java.util.HashMap;
import java.util.Iterator;

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
	
	
	public String toString()
	{
		String str = "[ ";
		Iterator<String> attrIter = valueSpaceBoundary.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			AttributeValueRange attrValRange = valueSpaceBoundary.get(attrName);
			
			str = str + "("+attrName+"["+attrValRange.getLowerBound()+","
							+attrValRange.getUpperBound()+"]) , ";
		}
		str =str + "]";
		return str;
	}
}