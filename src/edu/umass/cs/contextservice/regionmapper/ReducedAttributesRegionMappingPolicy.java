package edu.umass.cs.contextservice.regionmapper;

import java.util.HashMap;
import java.util.List;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;

/**
 * 
 * @author ayadav
 *
 */
public class ReducedAttributesRegionMappingPolicy<NodeIDType> 
					implements RegionMappingPolicyInterface<NodeIDType>
{
	@Override
	public List<NodeIDType> getNodeIDsForAValueSpace
			(HashMap<String, AttributeValueRange> valueSpaceDef) 
	{	
		return null;
	}
	
	
	@Override
	public void computeRegionMapping(
			HashMap<String, AttributeMetaInfo> attributeMap, 
			List<NodeIDType> nodeIDList)
	{
		
	}
}