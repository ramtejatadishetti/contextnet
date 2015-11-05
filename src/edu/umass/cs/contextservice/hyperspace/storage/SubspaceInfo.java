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
	private final HashMap<String, Boolean> attributesOfSubspace;
	private final Vector<NodeIDType> nodesOfSubspace;
	private final Vector<DomainPartitionInfo> domainPartitionInfo;
	
	public SubspaceInfo(int subspaceNum, HashMap<String, Boolean> attributesOfSubspace, 
			Vector<NodeIDType> nodesOfSubspace, Vector<DomainPartitionInfo> domainPartitionInfo)
	{
		this.subspaceNum 			= subspaceNum;
		this.attributesOfSubspace 	= attributesOfSubspace;
		this.nodesOfSubspace 		= nodesOfSubspace;
		this.domainPartitionInfo 	= domainPartitionInfo;
	}
	
	public HashMap<String, Boolean> getAttributesOfSubspace()
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
	
	public Vector<DomainPartitionInfo> getDomainPartitionInfo()
	{
		return this.domainPartitionInfo;
	}
}