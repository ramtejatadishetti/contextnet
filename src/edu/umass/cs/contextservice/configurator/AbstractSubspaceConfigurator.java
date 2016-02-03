package edu.umass.cs.contextservice.configurator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.nio.interfaces.NodeConfig;

public abstract class AbstractSubspaceConfigurator<NodeIDType>
{
	protected NodeConfig<NodeIDType> nodeConfig;
	
	// stores subspace info
	// key is the distinct subspace id and it stores all replicas of that subspace.
	// a replica of a subspace is defined over same attributes but different nodes
	// this map is written only once, in one thread,  and read many times, by many threads, so no need to make concurrent.
	protected  HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap;
	
	public AbstractSubspaceConfigurator(NodeConfig<NodeIDType> nodeConfig)
	{
		this.nodeConfig = nodeConfig;
		subspaceInfoMap = new HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>>();
	}
	
	public abstract void configureSubspaceInfo();
	
	protected void printSubspaceInfo()
	{
		Iterator<Integer> subspceIter = subspaceInfoMap.keySet().iterator();
		
		while( subspceIter.hasNext() )
		{
			int distinctSubId = subspceIter.next();
			
			Vector<SubspaceInfo<NodeIDType>> replicaVect = subspaceInfoMap.get(distinctSubId);
			System.out.println("number of replicas for subspaceid "+distinctSubId
					+" "+replicaVect.size());
			for(int i=0; i<replicaVect.size();i++)
			{
				SubspaceInfo<NodeIDType> currSubspace = replicaVect.get(i);
				System.out.println(currSubspace.toString());
			}
		}
	}
	
	public HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> getSubspaceInfoMap()
	{
		return this.subspaceInfoMap;
	}
}