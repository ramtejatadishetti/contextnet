package edu.umass.cs.contextservice.regionmapper.helper;

import java.util.List;

/**
 * Region info class. Stores information like the region boundaries and 
 * which next attribute to split if we want to split the region.
 * 
 * @author ayadav
 */
public class RegionInfo
{	
	private ValueSpaceInfo valSpaceInfo;
	
	private List<Integer> nodeList;
	
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
	
	public List<Integer> getNodeList()
	{
		return this.nodeList;
	}
	
	public void setNodeList(List<Integer> nodeList)
	{
		this.nodeList = nodeList;
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