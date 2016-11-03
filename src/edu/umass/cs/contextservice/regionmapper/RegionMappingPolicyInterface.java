package edu.umass.cs.contextservice.regionmapper;

import java.util.HashMap;
import java.util.List;

/**
 * This interface implements methods to implement a region mapping policy.
 * @author ayadav
 * 
 * @param <NodeIDType>
 */
public interface RegionMappingPolicyInterface<NodeIDType>
{
	/**
	 * This function computes the nodeIDs corresponding to regions that overlap
	 * with the valuespace defined in the input parameter. This function can be used 
	 * for both updates and searches. In updates, the lower and upper bound of an 
	 * attribute are same in AttributeValueRange class. In search, the lower and upper 
	 * bound specify the lower and upper bounds in a search query.
	 * @param valueSpaceDef
	 * @return
	 */
	public List<NodeIDType> 
		getNodeIDsForAValueSpace(
				HashMap<String, AttributeValueRange> valueSpaceDef);
}