package edu.umass.cs.contextservice.hyperspace.storage;

import java.util.HashMap;
import java.util.Vector;

/**
 * stores attributes that define a subspace
 * @author adipc
 */
public class SubspaceInfo<NodeIDType>
{
	private final int subspaceNum;
	private final HashMap<String, AttributePartitionInfo> attributesOfSubspace;
	private final Vector<NodeIDType> nodesOfSubspace;
	// right now num of paritions is same for each attribute 
	// in the subspace
	private final int numPartitions;
	
	public SubspaceInfo(int subspaceNum, HashMap<String, AttributePartitionInfo> attributesOfSubspace, 
			Vector<NodeIDType> nodesOfSubspace, int numPartitions)
	{
		this.subspaceNum 			= subspaceNum;
		this.attributesOfSubspace 	= attributesOfSubspace;
		this.nodesOfSubspace 		= nodesOfSubspace;
		this.numPartitions 			= numPartitions;
	}
	
	public HashMap<String, AttributePartitionInfo> getAttributesOfSubspace()
	{
		return this.attributesOfSubspace;
	}
	
	public Vector<NodeIDType> getNodesOfSubspace()
	{
		return this.nodesOfSubspace;
	}
	
	public int getSubspaceNum()
	{
		return this.subspaceNum;
	}
	
	public int getNumPartitions()
	{
		return this.numPartitions;
	}
}