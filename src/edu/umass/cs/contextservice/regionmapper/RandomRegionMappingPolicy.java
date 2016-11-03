package edu.umass.cs.contextservice.regionmapper;

import java.util.HashMap;
import java.util.List;

/**
 * This class implements a random region mapping policy.
 * @author ayadav
 *
 */
public class RandomRegionMappingPolicy<NodeIDType> 
					implements RegionMappingPolicyInterface<NodeIDType>
{
	@Override
	public List<NodeIDType> getNodeIDsForAValueSpace
			(HashMap<String, AttributeValueRange> valueSpaceDef) 
	{
		
		return null;
	}
}