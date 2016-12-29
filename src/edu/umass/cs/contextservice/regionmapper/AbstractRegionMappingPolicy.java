package edu.umass.cs.contextservice.regionmapper;

import java.util.HashMap;
import java.util.List;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;

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
	protected final CSNodeConfig nodeConfig;
	
	
	public AbstractRegionMappingPolicy( HashMap<String, AttributeMetaInfo> attributeMap, 
			CSNodeConfig nodeConfig )
	{
		this.attributeMap = attributeMap;
		this.nodeConfig = nodeConfig;
	}
	
	
	/**
	 * This function computes the nodeIDs corresponding to regions that overlap
	 * with the value space defined in the input parameter. This function can be used 
	 * for both updates and searches. In updates, the lower and upper bound of an 
	 * attribute are same in AttributeValueRange class. In search, the lower and upper 
	 * bound specify the lower and upper bounds in a search query.
	 * If the request type is set to update then all nodes corresponding to a region's value space 
	 * that overlap with the input value space are returned. If the request is set to search then 
	 * only one node corresponding to each region's value space that overlaps with the input value space is 
	 * returned.
	 * @param valueSpaceDef
	 * @return
	 */
	public abstract List<Integer> getNodeIDsForAValueSpaceForSearch
							(ValueSpaceInfo valueSpace);
	
	
	public abstract List<Integer> getNodeIDsForAValueSpaceForUpdate
					(String GUID, ValueSpaceInfo valueSpace);
	
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