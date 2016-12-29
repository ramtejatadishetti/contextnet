package edu.umass.cs.contextservice.hyperspace.storage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import edu.umass.cs.contextservice.regionmapper.helper.RegionInfo;

/**
 * Stores attributes that define a subspace
 * @author adipc
 */
public class SubspaceInfo
{
	// this is the distinct subspace id for a subspace
	// replicated subspaces will have this as same.
	private final int subspaceId;
	
	// replica num for the given subspace
	private final int replicaNum;
	
	private final HashMap<String, AttributePartitionInfo> attributesOfSubspace;
	private final List<Integer> nodesOfSubspace;
	
	private final List<RegionInfo> subspaceRegionList;
	
	// right now num of paritions is same for each attribute 
	// in the subspace
	private  int numPartitions;
	
	public SubspaceInfo(int subspaceId, int replicaNum, 
			HashMap<String, AttributePartitionInfo> attributesOfSubspace, 
			List<Integer> nodesOfSubspace)
	{
		this.subspaceId 			= subspaceId;
		this.replicaNum 			= replicaNum;
		this.attributesOfSubspace 	= attributesOfSubspace;
		this.nodesOfSubspace 		= nodesOfSubspace;
		subspaceRegionList          = new LinkedList<RegionInfo>();
	}
	
	public HashMap<String, AttributePartitionInfo> getAttributesOfSubspace()
	{
		return this.attributesOfSubspace;
	}
	
	public int getSubspaceId()
	{
		return this.subspaceId;
	}
	
	public int getReplicaNum()
	{
		return this.replicaNum;
	}
	
	public void addRegionToList(RegionInfo region)
	{
		this.subspaceRegionList.add(region);
	}
	
	public List<RegionInfo> getSubspaceRegionsList()
	{
		return subspaceRegionList;
	}
	
	public String toString()
	{
		String str = "subspace id "+this.subspaceId+" replica num "+this.replicaNum+
				" attributes ";
		
		Iterator<String> attrIter = this.attributesOfSubspace.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			str = str +attrIter.next()+" ";
		}
		str = str+" nodes ";
		for(int i=0; i<nodesOfSubspace.size(); i++)
		{
			str = str + nodesOfSubspace.get(i)+" ";
		}
		return str;
	}
	
	public List<Integer> getNodesOfSubspace()
	{
		return this.nodesOfSubspace;
	}
	
	public void setNumPartitions(int numPartitions)
	{
		this.numPartitions = numPartitions;
	}
	
	public int getNumPartitions()
	{
		return numPartitions;
	}
	
	/**
	 * checks if the subspace nodes have my id.
	 * @return
	 */
	public boolean checkIfSubspaceHasMyID(Integer idToCheck)
	{
		for(int i=0;i<nodesOfSubspace.size();i++)
		{
			Integer currID = nodesOfSubspace.get(i);
			if(Integer.parseInt(currID+"") == Integer.parseInt(idToCheck+""))
			{
				return true;
			}
		}
		return false;
	}
}