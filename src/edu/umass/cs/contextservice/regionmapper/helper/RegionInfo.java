package edu.umass.cs.contextservice.regionmapper.helper;

import java.util.Iterator;

/**
 * Region info class. Stores information like the region boundaries and 
 * which next attribute to split if we want to split the region.
 * 
 * @author ayadav
 */
public class RegionInfo
{	
	private ValueSpaceInfo valSpaceInfo;
	
	private int assignedNodeId;
	
	private double traceLoad;
	
	public RegionInfo()
	{
	}
	
	public ValueSpaceInfo getValueSpaceInfo()
	{
		return this.valSpaceInfo;
	}
	
	public void setValueSpaceInfo(ValueSpaceInfo valSpaceInfo)
	{
		this.valSpaceInfo = valSpaceInfo;
	}
	
	public int getAssignedNodeId()
	{
		return this.assignedNodeId;
	}
	
	public void setTraceLoad(double traceLoad)
	{
		this.traceLoad = traceLoad;
	}
	
	public double getTraceLoad()
	{
		return traceLoad;
	}
	
	public String toString()
	{
		return valSpaceInfo.toString();
	}
}