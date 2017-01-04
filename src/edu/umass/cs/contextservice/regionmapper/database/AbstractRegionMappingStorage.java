package edu.umass.cs.contextservice.regionmapper.database;

import java.util.HashMap;
import java.util.List;

import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;
import edu.umass.cs.contextservice.regionmapper.helper.RegionInfo;

public abstract class AbstractRegionMappingStorage 
{
	
	public abstract void createTables();
	
	// this one checks the range overlap
	public abstract List<Integer> getNodeIdsForSearch
					(String tableName, HashMap<String, AttributeValueRange> attrValRangeMap);
	
	
	// this one checks the the update value falls in the range.
	public abstract List<Integer> getNodeIdsForUpdate
						(String tableName, HashMap<String, AttributeValueRange> attrValRangeMap);
	
	
	public abstract void insertRegionInfoIntoTable(String tableName, 
					RegionInfo regionInfo);
}