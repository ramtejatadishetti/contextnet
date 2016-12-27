package edu.umass.cs.contextservice.regionmapper.helper;

import java.util.Comparator;

public class RegionLoadComparator implements Comparator<RegionInfo>
{
	public int compare(RegionInfo o1, RegionInfo o2)
	{
		if(o2.getTraceLoad() >= o1.getTraceLoad())
		{
			return 1;
		}
		else
		{
			return -1;
		}
	}
}