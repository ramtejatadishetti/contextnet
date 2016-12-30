package edu.umass.cs.contextservice.regionmapper.database;

import java.util.List;

import edu.umass.cs.contextservice.regionmapper.helper.RegionInfo;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;

public abstract class AbstractRegionMappingStorage 
{
	
	public abstract void createTables();
	
	public abstract List<List<Integer>> getNodeIdsForValueSpace
						(String tableName, ValueSpaceInfo valSpaceInfo);
	
	
	public abstract void insertRegionInfoIntoTable(String tableName, 
					RegionInfo regionInfo);
}