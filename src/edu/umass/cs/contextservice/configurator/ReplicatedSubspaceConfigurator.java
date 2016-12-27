package edu.umass.cs.contextservice.configurator;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.datasource.AbstractDataSource;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
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
public class ReplicatedSubspaceConfigurator extends AbstractSubspaceConfigurator
{
	// each domain is at least partitioned into two.
	//TODO: these values will be determined by the model at some point
	
	private final double optimalH;
	public ReplicatedSubspaceConfigurator(NodeConfig<Integer> nodeConfig, 
			int optimalH, AbstractDataSource dataSource)
	{
		super(nodeConfig, dataSource);
		this.optimalH = optimalH;
	}
	
	@Override
	public void configureSubspaceInfo()
	{
		double numNodes = nodeConfig.getNodeIDs().size();
		double numAttrs = AttributeTypes.attributeMap.size();
		
		ContextServiceLogger.getLogger().fine("double divide "+numAttrs/optimalH+" numAttrs "+numAttrs
				+" optimalH "+optimalH);
		double numSubspaces = Math.ceil(numAttrs/optimalH);
		
		// number of subspaces can't be greater than number of nodes.
		// this case happens mostly when number of nodes=1
//		if( numSubspaces > numNodes )
//		{
//			numSubspaces = numNodes;
//			optimalH = Math.ceil(numAttrs/numSubspaces);
//		}
		
		// N/S(N) is from the model, we consider S(N) to be sqrt(N)
		// so N/S(N) is sqrt(N)
		double nBySN = Math.sqrt(numNodes);
		int numberOfTotalSubspaces = (int) Math.floor(nBySN);
		int minNumOfNodesToSubspace = (int) Math.floor(nBySN);
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
				assignNodesUniformlyToSubspaces(minNumOfNodesToSubspace, (int)numSubspaces);

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
			
			if(numberNodesForSubspace > 0)
			{
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
						Vector<SubspaceInfo> currSubVect
											= subspaceInfoMap.get(subspaceKey);
						assert( currSubVect.size() > 0 );
						currSubVect.get(0).getNodesOfSubspace().add( (Integer)(Integer)nodesIdCounter );
						nodesIdCounter++;
					}
					else
					{
						subspaceKeyIter = subspaceInfoMap.keySet().iterator();
					}
				}
			}
			else // numnodes are less than number of subspaces, so just assign each node to each subspace
			{
				// first the basic nodes are assigned then remaining nodes are assigned 
				// uniformly to the existing subspaces.
					
				int nodesIdCounter = assignNodesUniformlyToSubspaces(numberNodesForSubspace, (int)numSubspaces);
				// above will not be able to assign any nodes
				assert(nodesIdCounter == 0);
				
				Iterator<Integer> subspaceKeyIter = subspaceInfoMap.keySet().iterator();
		
				while( nodesIdCounter < numNodes )
				{
					int subspaceKey = -1;
					// assigning each node to each subspace
					while( subspaceKeyIter.hasNext() )
					{
						subspaceKey = subspaceKeyIter.next();
						Vector<SubspaceInfo> currSubVect
											= subspaceInfoMap.get(subspaceKey);
						assert( currSubVect.size() > 0 );
						currSubVect.get(0).getNodesOfSubspace().add( (Integer)(Integer)nodesIdCounter );
					}
					nodesIdCounter++;
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
			
			Vector<Integer> subspaceNodes = new Vector<Integer>();
			HashMap<String, AttributePartitionInfo> subspaceAttrs 
									= new HashMap<String, AttributePartitionInfo>();
			
			for(int j=0; j<numberNodesForSubspace; j++)
			{
				subspaceNodes.add( (Integer)(Integer)nodesIdCounter );
				nodesIdCounter++;
			}
			
			int numCurrAttr = 0;
			
			while( (numCurrAttr < optimalH) && (attrIndexCounter < attrVector.size()) )
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
			SubspaceInfo currSubInfo 
				= new SubspaceInfo( distinctSubspaceId, 0, subspaceAttrs, subspaceNodes );
			
			Vector<SubspaceInfo> replicatedSubspacesVector 
						= new Vector<SubspaceInfo>(); 
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
				SubspaceInfo currSubspace = subspaceInfoMap.get(distinctSubId).get(0);
				
				Vector<Integer> replicatedSubspaceNodes = new Vector<Integer>();
				
				for( int i=0;i<intSqrtNodes;i++ )
				{
					replicatedSubspaceNodes.add((Integer)((Integer)nodesIdCounter));
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
				
				SubspaceInfo newReplicatedSubspace 
					= new SubspaceInfo( distinctSubId, replicaNum,
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
				Vector<SubspaceInfo> subspaceReplicaVect 
						= subspaceInfoMap.get(distinctSubspaceId);
				int actualVectIndex = replicaNum%subspaceReplicaVect.size();
				subspaceReplicaVect.get(actualVectIndex).
				getNodesOfSubspace().add((Integer)((Integer)nodesIdCounter));
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
		
		CSNodeConfig testNodeConfig = new CSNodeConfig();
		int startingPort = 5000;
		for( int i=0;i<numberOfNodes;i++ )
		{
			InetSocketAddress sockAddr 
					= new InetSocketAddress("127.0.0.1", startingPort+i);
			testNodeConfig.add(i, sockAddr);
		}
		
		ReplicatedSubspaceConfigurator subspaceConfigurator 
								= new ReplicatedSubspaceConfigurator(testNodeConfig, 2, null);
		subspaceConfigurator.configureSubspaceInfo();
		
		subspaceConfigurator.printSubspaceInfo();
	}
}


/*private void initializePartitionInfo()
{
	Iterator<Integer> keyIter = subspaceInfoMap.keySet().iterator();
	while( keyIter.hasNext() )
	{
		Vector<SubspaceInfo<Integer>> currSubVect 
	 		= subspaceInfoMap.get(keyIter.next());
		
		for( int i=0; i<currSubVect.size(); i++ )
		{
			SubspaceInfo<Integer> currSubInfo = currSubVect.get(i);
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