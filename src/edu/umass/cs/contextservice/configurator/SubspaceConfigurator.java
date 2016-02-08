package edu.umass.cs.contextservice.configurator;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.nio.interfaces.NodeConfig;

/**
 * Used to configure subspace configuration 
 * based on number of nodes, attributes.
 * It follows sqrt(N) model while configuring subspaces.
 * It partitions each attribute into two partitions at least.
 * It also decides subspace configuration like which attributes 
 * go into which subspaces.
 * @author adipc
 */
public class SubspaceConfigurator<NodeIDType> extends AbstractSubspaceConfigurator<NodeIDType>
{
	// each domain is at least partitioned into two.
	//TODO: these values will be determined by the model at some point
	//private static final double MIN_P = 2;
	private static final double MAX_H = 3;
	
	public SubspaceConfigurator(NodeConfig<NodeIDType> nodeConfig)
	{
		super(nodeConfig);
	}
	
	@Override
	public void configureSubspaceInfo()
	{
		double numNodes = nodeConfig.getNodeIDs().size();
		double numAttrs = AttributeTypes.attributeMap.size();
		//TODO: later on h value, num of attrs in a subspace will be calculated by the model
		double numSubspaces = Math.ceil(numAttrs/MAX_H);
		
		
		// N/S(N) is from the model, we consider S(N) to be sqrt(N)
		// so N/S(N) is sqrt(N)
		double nBySN = Math.sqrt(numNodes);
		int numberOfTotalSubspaces = (int) Math.floor(nBySN);
		int mimNumOfNodesToSubspace = (int) Math.floor(nBySN);
		// we need to replicate existing subspaces
		// unless nBySN that is the number of nodes assigned to a subspace is greater 
		// than MIN_P^attrs, till then no point in replicating. we want p to 
		// be minimum 2.
		if( (numberOfTotalSubspaces > numSubspaces) 
				/*&& (mimNumOfNodesToSubspace >= Math.pow(MIN_P, Math.min(numAttrs, MAX_H)))*/ )
		{
			// assign sqrt(N) nodes to each subspace and then if more than sqrt(N) 
			// nodes are remaining then replicate one subspace or if less are remaining 
			// then just add it to the current subspaces.
			
			int nodesIdCounter = 
				assignNodesUniformlyToSubspaces(mimNumOfNodesToSubspace, (int)numSubspaces);

			nodesIdCounter = replicateSubspaces(nodesIdCounter, (int) numNodes);
			// all nodes should be assigned by now
			assert( (numNodes-nodesIdCounter) == 0);
		}
		else
		{
			Vector<AttributeMetaInfo> attrVector 
								= new Vector<AttributeMetaInfo>();
			attrVector.addAll(AttributeTypes.attributeMap.values());
			
			double numberNodesForSubspace = Math.floor(numNodes/numSubspaces);
			
			// first the basic nodes are assigned then remaining nodes are assigned 
			// uniformly to the existing subspaces.
			
			int nodesIdCounter = assignNodesUniformlyToSubspaces(numberNodesForSubspace, (int)numSubspaces);
			
			//double remainingNodes = numNodes - numberNodesForSubspace*numSubspaces;
			
			Iterator<Integer> subspaceKeyIter = subspaceInfoMap.keySet().iterator();
			
			while( nodesIdCounter < numNodes )
			{
				int subspaceKey = -1;
				
				if( subspaceKeyIter.hasNext() )
				{
					subspaceKey = subspaceKeyIter.next();
					Vector<SubspaceInfo<NodeIDType>> currSubVect
					 		= subspaceInfoMap.get(subspaceKey);
					assert( currSubVect.size() > 0 );
					currSubVect.get(0).getNodesOfSubspace().add( (NodeIDType)(Integer)nodesIdCounter );
					nodesIdCounter++;
				}
				else
				{
					subspaceKeyIter = subspaceInfoMap.keySet().iterator();
				}
			}
		}
		// initializes the domain partitions for an attribute
		initializePartitionInfo();
		this.printSubspaceInfo();
		
		//TODO: add assertion to check that one node lies in only one replica of a subspace
		// this is necessary for the remaining code to work.
	}
	
	/**
	 * This function assigns nodes uniformly to subspaces
	 * and stores them in the inherited subspaceInfoVector 
	 * vector.
	 * returns the nodeId of the first unassigned node
	 * nodeId <= numNodes
	 */
	private int assignNodesUniformlyToSubspaces
			(double numberNodesForSubspace, int numSubspaces)
	{
		Vector<AttributeMetaInfo> attrVector = new Vector<AttributeMetaInfo>();
		attrVector.addAll(AttributeTypes.attributeMap.values());
		
		//double numberNodesForSubspace = Math.floor(numNodes/numSubspaces);
		
		// first the basic nodes are assigned then remaining nodes are assigned 
		// uniformly to the existing subspaces.
		
		int nodesIdCounter   = 0;
		int attrIndexCounter = 0;
		
		for(int i=0; i<numSubspaces; i++)
		{
			int distinctSubspaceId 	= i;
			
			Vector<NodeIDType> subspaceNodes = new Vector<NodeIDType>();
			HashMap<String, AttributePartitionInfo> subspaceAttrs 
									= new HashMap<String, AttributePartitionInfo>();
			
			for(int j=0; j<numberNodesForSubspace; j++)
			{
				subspaceNodes.add( (NodeIDType)(Integer)nodesIdCounter );
				nodesIdCounter++;
			}
			
			int numCurrAttr = 0;
			
			while( (numCurrAttr < MAX_H) && (attrIndexCounter < attrVector.size()) )
			{
				AttributeMetaInfo currAttrMetaInfo = attrVector.get(attrIndexCounter);
				String attrName = currAttrMetaInfo.getAttrName();
				AttributePartitionInfo attrPartInfo = new AttributePartitionInfo
						( currAttrMetaInfo );
				subspaceAttrs.put(attrName, attrPartInfo);
				numCurrAttr++;
				attrIndexCounter++;
			}
			
			// replica num 0 as first replica is created
			SubspaceInfo<NodeIDType> currSubInfo 
				= new SubspaceInfo<NodeIDType>( distinctSubspaceId, 0, subspaceAttrs, subspaceNodes );
			
			Vector<SubspaceInfo<NodeIDType>> replicatedSubspacesVector 
						= new Vector<SubspaceInfo<NodeIDType>>(); 
			replicatedSubspacesVector.add(currSubInfo);
			
			subspaceInfoMap.put(distinctSubspaceId, replicatedSubspacesVector);
		}
		//all attributes should be assigned to some subspace by end of this function.
		assert(attrIndexCounter == attrVector.size() );
		return nodesIdCounter;
	}
	
	/**
	 * returns the total number of subspaces, including replica subspaces
	 * @return
	 */
	private int getTotalSubspaces()
	{
		int totalSub = 0;
		Iterator<Integer> mapIter = subspaceInfoMap.keySet().iterator();
		while( mapIter.hasNext() )
		{
			totalSub = totalSub + subspaceInfoMap.get(mapIter.next()).size();
		}
		return totalSub;
	}
	
	/**
	 * Creates a replicated subspace info after 
	 * initializing subspaces in the beginning.
	 * 
	 * @param nodesIdCounter
	 * @return nodeIDCounter, it should be set to numNodes, 
	 * all nodes should have been assigned by now
	 */
	private int replicateSubspaces( int nodesIdCounter, int numNodes )
	{
		int remainingNodes  = numNodes - nodesIdCounter;
		int numberOfTotalSubspaces = (int) Math.floor(Math.sqrt(numNodes));
		int intSqrtNodes    = (int) Math.floor(Math.sqrt(numNodes));
		
		Iterator<Integer> mapIter = subspaceInfoMap.keySet().iterator();
		// 0th or first replica already created
		int replicaNum = 1;
		while( remainingNodes >= intSqrtNodes )
		{
			// maximum number of subspaces created, now remaining
			// nodes will be uniformly distributed.
			if(getTotalSubspaces() >= numberOfTotalSubspaces)
			{
				break;
			}
			
			if( mapIter.hasNext() )
			{
				int distinctSubId = mapIter.next();
				SubspaceInfo<NodeIDType> currSubspace = subspaceInfoMap.get(distinctSubId).get(0);
				
				Vector<NodeIDType> replicatedSubspaceNodes = new Vector<NodeIDType>();
				
				for( int i=0;i<intSqrtNodes;i++ )
				{
					replicatedSubspaceNodes.add((NodeIDType)((Integer)nodesIdCounter));
					nodesIdCounter++;
				}
				
				Iterator<String> currSubpaceAttrIter = 
						currSubspace.getAttributesOfSubspace().keySet().iterator();
				
				HashMap<String, AttributePartitionInfo> replicatedSubspaceAttrs
						= new HashMap<String, AttributePartitionInfo>();
				
				// not just copying the pointer of currSubpace attr 
				// info in the replicated one
				while( currSubpaceAttrIter.hasNext() )
				{
					String attrName = currSubpaceAttrIter.next();
					AttributeMetaInfo currMetaInfo = AttributeTypes.attributeMap.get(attrName);
					AttributePartitionInfo attrPartInfo = new AttributePartitionInfo
							( currMetaInfo );
					replicatedSubspaceAttrs.put(attrName, attrPartInfo);
				}
				
				SubspaceInfo<NodeIDType> newReplicatedSubspace 
					= new SubspaceInfo<NodeIDType>( distinctSubId, replicaNum,
							replicatedSubspaceAttrs, replicatedSubspaceNodes );
				
				// adding the new replicated subspace in the vector
				subspaceInfoMap.get(distinctSubId).add(newReplicatedSubspace);
				remainingNodes  = numNodes - nodesIdCounter;
			}
			else
			{
				mapIter = subspaceInfoMap.keySet().iterator();
				replicaNum++;
			}
		}
		
		// assign the remaining nodes
		
		// replicaNum denotes which replica of a subspace we are assigning the
		// remaining nodes
		replicaNum = 0;
		
		mapIter = subspaceInfoMap.keySet().iterator();
		
		while(nodesIdCounter < numNodes)
		{
			if( mapIter.hasNext() )
			{
				int distinctSubspaceId = mapIter.next();
				Vector<SubspaceInfo<NodeIDType>> subspaceReplicaVect 
						= subspaceInfoMap.get(distinctSubspaceId);
				int actualVectIndex = replicaNum%subspaceReplicaVect.size();
				subspaceReplicaVect.get(actualVectIndex).
				getNodesOfSubspace().add((NodeIDType)((Integer)nodesIdCounter));
				nodesIdCounter++;
			}
			else
			{
				mapIter = subspaceInfoMap.keySet().iterator();
				replicaNum++;
			}
		}
		assert(nodesIdCounter == numNodes);
		assert(getTotalSubspaces() <= numberOfTotalSubspaces);
		return nodesIdCounter;
	}
	
	// test the above class. 
	public static void main(String[] args)
	{
		int numberOfNodes = Integer.parseInt(args[0]);
		
		ContextServiceConfig.configFileDirectory = "conf/singleNodeConf/contextServiceConf";
		AttributeTypes.initialize();
		
		CSNodeConfig<Integer> testNodeConfig = new CSNodeConfig<Integer>();
		int startingPort = 5000;
		for( int i=0;i<numberOfNodes;i++ )
		{
			InetSocketAddress sockAddr 
					= new InetSocketAddress("127.0.0.1", startingPort+i);
			testNodeConfig.add(i, sockAddr);
		}
		
		SubspaceConfigurator<Integer> subspaceConfigurator 
								= new SubspaceConfigurator<Integer>(testNodeConfig);
		subspaceConfigurator.configureSubspaceInfo();
		
		subspaceConfigurator.printSubspaceInfo();
	}
}


/*private void initializePartitionInfo()
{
	Iterator<Integer> keyIter = subspaceInfoMap.keySet().iterator();
	while( keyIter.hasNext() )
	{
		Vector<SubspaceInfo<NodeIDType>> currSubVect 
	 		= subspaceInfoMap.get(keyIter.next());
		
		for( int i=0; i<currSubVect.size(); i++ )
		{
			SubspaceInfo<NodeIDType> currSubInfo = currSubVect.get(i);
			int currSubspaceNumNodes = currSubInfo.getNodesOfSubspace().size();
			int currSubspaceNumAttrs = currSubInfo.getAttributesOfSubspace().size();
			
			int currSubspaceNumPartitions 
				= (int)Math.ceil(Math.pow(currSubspaceNumNodes, 1.0/currSubspaceNumAttrs));
			currSubInfo.setNumPartitions(currSubspaceNumPartitions);
			
			Iterator<String> subspaceAttrIter
								= currSubInfo.getAttributesOfSubspace().keySet().iterator();
			while( subspaceAttrIter.hasNext() )
			{
				String attrName = subspaceAttrIter.next();
				AttributePartitionInfo attrPartInfo 
						= currSubInfo.getAttributesOfSubspace().get(attrName);
				attrPartInfo.initializePartitionInfo(currSubspaceNumPartitions);
			}
		}
	}
}*/