package edu.umass.cs.contextservice.hyperspace.storage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

/**
 * Stores attributes that define a subspace
 * @author adipc
 */
public class SubspaceInfo
{
	// this is the distict subspace id for a subspace
	// replicated subspaces will have this as same.
	private final int subspaceId;
	
	// replica num for the given subspace
	private final int replicaNum;
	
	private final HashMap<String, AttributePartitionInfo> attributesOfSubspace;
	private final Vector<Integer> nodesOfSubspace;
	// right now num of paritions is same for each attribute 
	// in the subspace
	private int numPartitions;
	
	public SubspaceInfo(int subspaceId, int replicaNum, HashMap<String, AttributePartitionInfo> attributesOfSubspace, 
			Vector<Integer> nodesOfSubspace)
	{
		this.subspaceId 			= subspaceId;
		this.replicaNum 			= replicaNum;
		this.attributesOfSubspace 	= attributesOfSubspace;
		this.nodesOfSubspace 		= nodesOfSubspace;
		//this.numPartitions 		= numPartitions;
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
	
	public int getNumPartitions()
	{
		return this.numPartitions;
	}
	
	public void setNumPartitions( int numPartitions )
	{
		this.numPartitions = numPartitions;
	}
	
	public String toString()
	{
		String str = "subspace id "+this.subspaceId+" replica num "+this.replicaNum+
				" num partitions "+ numPartitions +" attributes ";
		
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
	
	public Vector<Integer> getNodesOfSubspace()
	{
		return this.nodesOfSubspace;
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