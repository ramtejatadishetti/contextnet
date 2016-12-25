package edu.umass.cs.contextservice.regionmapper;

import java.util.HashMap;
import java.util.List;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;

/**
 * 
 * This interface implements methods to implement 
 * a region mapping policy.
 * @author ayadav
 * @param <Integer>
 */
public abstract class AbstractRegionMappingPolicy
{
	protected final HashMap<String, AttributeMetaInfo> attributeMap;
	protected final List<Integer> nodeIDList;
	
	
	public AbstractRegionMappingPolicy( HashMap<String, AttributeMetaInfo> attributeMap, 
			List<Integer> nodeIDList )
	{
		this.attributeMap = attributeMap;
		this.nodeIDList = nodeIDList;
	}
	
	
	/**
	 * This function computes the nodeIDs corresponding to regions that overlap
	 * with the value space defined in the input parameter. This function can be used 
	 * for both updates and searches. In updates, the lower and upper bound of an 
	 * attribute are same in AttributeValueRange class. In search, the lower and upper 
	 * bound specify the lower and upper bounds in a search query.
	 * @param valueSpaceDef
	 * @return
	 */
	public abstract List<Integer> getNodeIDsForAValueSpace(
				HashMap<String, AttributeValueRange> valueSpaceDef);
	
	
	/**
	 * This function computes the region mapping. This function can use 
	 * any scheme to compute the region mapping, like creating a hyperspace
	 * of all attributes and map regions to nodes in that hyperspace or creating 
	 * a multiple subspaces of subsets of attributes and then map region to nodes
	 * in each subspace.
	 * The class implementing this interface can store the region to node
	 * mapping in a database or can keep that in memory or compute on fly.
	 * 
	 * @param attributeMap
	 * @param nodeIDList
	 */
	public abstract void computeRegionMapping();
}