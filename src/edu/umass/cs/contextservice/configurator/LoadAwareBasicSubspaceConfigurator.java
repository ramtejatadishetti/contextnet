package edu.umass.cs.contextservice.configurator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes.DomainPartitionInfo;
import edu.umass.cs.contextservice.configurator.helperclasses.PartitionLoadReporting;
import edu.umass.cs.contextservice.configurator.helperclasses.PartitionToNodeInfo;
import edu.umass.cs.contextservice.configurator.helperclasses.RangeInfo;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.nio.interfaces.NodeConfig;

public class LoadAwareBasicSubspaceConfigurator 
							extends AbstractSubspaceConfigurator
{
	// load map , key is nodeId, and value is number of requests/s
	//private HashMap<Integer, Double> loadMap;
	private final double optimalH;
	
	private HashMap<Integer, Vector<SubspaceInfo>> oldSubspaceInfoMap;
	//private HashMap<String, PartitionToNodeInfo> partitionToNodeMap;
	
	// String key is the old subspaceId-replicaNum
	//private HashMap<String, List<PartitionLoadReporting>> partitionLoadMap;
	
	
	public LoadAwareBasicSubspaceConfigurator( NodeConfig<Integer> nodeConfig
			, int optimalH, HashMap<String, List<PartitionLoadReporting>> partitionLoadMap )
	{
		super(nodeConfig);
		
		oldSubspaceInfoMap 
				= new HashMap<Integer, Vector<SubspaceInfo>>();
		this.optimalH = optimalH;
		// every node load should be non zero. 
		// so if the load is zero we make it to 1.
		// load is measured in requests/s
//		Iterator<Integer> nodeIter = loadMap.keySet().iterator();
//		
//		while( nodeIter.hasNext() )
//		{
//			Integer nodeId = nodeIter.next();
//			double nodeLoad = loadMap.get(nodeId);
//			if( nodeLoad == 0.0 )
//			{
//				loadMap.put(nodeId, 1.0);
//			}
//		}
		
		//this.partitionLoadMap = partitionLoadMap;
		
		//partitionToNodeMap = new HashMap<String, PartitionToNodeInfo>();
	}
	
	@Override
	public void configureSubspaceInfo()
	{
		double numAttrs = AttributeTypes.attributeMap.size();
		double numNodes = nodeConfig.getNodeIDs().size();
		int numberOfSubspaces = (int)Math.floor(numAttrs/optimalH);
		int numNodesPerSubspace = (int)Math.floor(numNodes/numberOfSubspaces);
		
	}
	
	private void loadAwareSubspaceConfiguration()
	{
		// first assign nodes to subspaces, subspaces to nodes, based on load.
		HashMap<Integer, Double> oldSubspaceLoadMap = getOldSubspaceLoadMap();
		
		double totalLoad = getTotalLoadOnSystem();
		double numNodes  = nodeConfig.getNodeIDs().size();
		double loadPerNode = totalLoad/numNodes;
		
		HashMap<Integer, Double> nodeAssignmentLoadMap 
											= new HashMap<Integer, Double>();
		
		Set<Integer> nodeIds = nodeConfig.getNodeIDs();
		Iterator<Integer> nodeIdIter = nodeIds.iterator();
		
		while( nodeIdIter.hasNext() )
		{
			Integer nodeId = nodeIdIter.next();
			nodeAssignmentLoadMap.put(nodeId, loadPerNode);
		}
		
		loadAwareSubspacesToNodesAssignment(nodeAssignmentLoadMap, oldSubspaceLoadMap);
		
		// second assign nodes within subspaces, region to nodes mapping, based on load.
		
		
		
	}
	
	
	/**
	 * Recursive function to generate all the
	 * subspace regions/partitions.
	 */
	public HashMap<String, List<PartitionToNodeInfo>> 
										generateOldSubspacePartitions()
	{
		// key to map is subsapceId-replicaNum
		HashMap<String, List<PartitionToNodeInfo>> fullPartitionMap 
					= new HashMap<String, List<PartitionToNodeInfo>>();
		
		Iterator<Integer> subspaceIter = oldSubspaceInfoMap.keySet().iterator();
		
		while( subspaceIter.hasNext() )
		{
			int subspaceId = subspaceIter.next();
			Vector<SubspaceInfo> replicaVect 
								= oldSubspaceInfoMap.get(subspaceId);
			
			for( int i=0; i<replicaVect.size(); i++ )
			{
				SubspaceInfo subspaceInfo = replicaVect.get(i);
				HashMap<String, AttributePartitionInfo> attrsOfSubspace 
										= subspaceInfo.getAttributesOfSubspace();
				
				Vector<Integer> nodesOfSubspace 
					= subspaceInfo.getNodesOfSubspace();
				
				List<PartitionToNodeInfo> partitionList = 
						new LinkedList<PartitionToNodeInfo>();
				
				double numAttr  = attrsOfSubspace.size();
				//double numNodes = nodesOfSubspace.size();
				ContextServiceLogger.getLogger().fine(" NumPartitions "
												+subspaceInfo.getNumPartitions() );
				
				Integer[] partitionNumArray 
						= new Integer[subspaceInfo.getNumPartitions()];
				for(int j = 0; j<partitionNumArray.length; j++)
				{
					partitionNumArray[j] = new Integer(j);
				}
				
				ICombinatoricsVector<Integer> originalVector 
									= Factory.createVector(partitionNumArray);
				
			    //ICombinatoricsVector<Integer> originalVector = Factory.createVector(new String[] { "apple", "orange" });

				// Create the generator by calling the appropriate method in the Factory class. 
				// Set the second parameter as 3, since we will generate 3-elemets permutations
				Generator<Integer> gen 
								= Factory.createPermutationWithRepetitionGenerator
									(originalVector, (int)numAttr);
				
				// Print the result
				int nodeIdCounter = 0;
				int sizeOfNumNodes = nodesOfSubspace.size();
				
				for( ICombinatoricsVector<Integer> perm : gen )
				{
					Integer respNodeId 
						= nodesOfSubspace.get(nodeIdCounter%sizeOfNumNodes);
					
					HashMap<String, RangeInfo> attrBounds = 
							convertPermToBounds
							( perm.getVector(), attrsOfSubspace );
					
					//subspaceVectList.add(perm.getVector());
					//respNodeIdList.add(respNodeId);
					PartitionToNodeInfo partitionInfo 
								= new PartitionToNodeInfo
									( subspaceId, subspaceInfo.getReplicaNum(), 
												attrBounds, respNodeId );
					
					partitionList.add(partitionInfo);
					nodeIdCounter++;
				}
				
				String key = subspaceId+"-"+subspaceInfo.getReplicaNum();
				fullPartitionMap.put(key, partitionList);
			}
		}
		ContextServiceLogger.getLogger().fine
							(" generateOldSubspacePartitions() completed " );
		
		return fullPartitionMap;
	}
	
	private HashMap<String, RangeInfo> convertPermToBounds
			( List<Integer> permList, 
					HashMap<String, AttributePartitionInfo> attrSubspace )
	{
		HashMap<String, RangeInfo> attrBound 
							= new HashMap<String, RangeInfo>();
		
		Iterator<String> attrIter = attrSubspace.keySet().iterator();
		int counter = 0;
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			AttributePartitionInfo attrPartInfo = attrSubspace.get(attrName);
			int partitionNum = permList.get(counter);
			DomainPartitionInfo domainPartInfo 
				= attrPartInfo.getSubspaceDomainPartitionInfo().get(partitionNum);
			// if it is a String then single quotes needs to be added
			
			AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
			String dataType = attrMetaInfo.getDataType();
			
			
			String lowerBound  = AttributeTypes.convertStringToDataTypeForMySQL
							(domainPartInfo.lowerbound, dataType)+"";
			String upperBound  = AttributeTypes.convertStringToDataTypeForMySQL
							(domainPartInfo.upperbound, dataType)+"";
			
			RangeInfo rInfo = new RangeInfo(lowerBound, upperBound);
			
			attrBound.put(attrName, rInfo);
			counter++;
		}
		return attrBound;
	}
	
	private HashMap<Integer, List<Integer>> loadAwareSubspacesToNodesAssignment
			( HashMap<Integer, Double> nodeAssignmentLoadMap,
					HashMap<Integer, Double> oldSubspaceLoadMap	)
	{
		HashMap<Integer, List<Integer>> subspaceNodeAssignment 
										= new HashMap<Integer, List<Integer>>();
		
		Iterator<Integer> subspaceIdIter = oldSubspaceLoadMap.keySet().iterator();
		
		while( subspaceIdIter.hasNext() )
		{
			int subspaceId = subspaceIdIter.next();
			double totalSubspaceLoad = oldSubspaceLoadMap.get(subspaceId);
			List<Integer> subspaceNodeList 
					= assignNodesToASubspace( totalSubspaceLoad, nodeAssignmentLoadMap );
			
			subspaceNodeAssignment.put(subspaceId, subspaceNodeList);
		}
		return subspaceNodeAssignment;
	}
	
	
	private List<Integer> assignNodesToASubspace( double totalSubspaceLoad, 
			HashMap<Integer, Double> nodeAssignmentLoadMap )
	{
		List<Integer> nodeAssignmentList = new LinkedList<Integer>();
		Iterator<Integer> nodeIdIter = nodeAssignmentLoadMap.keySet().iterator();
		
		double curSubspaceLoad = totalSubspaceLoad;
		while( nodeIdIter.hasNext() )
		{
			Integer nodeId = nodeIdIter.next();
			double remainingNodeLoad = nodeAssignmentLoadMap.get(nodeId);
			
			// at least 1 node will be assigned even if the curSubspaceLoad=0
			if( remainingNodeLoad > 0.0 )
			{
				nodeAssignmentList.add(nodeId);
				if( curSubspaceLoad >= remainingNodeLoad )
				{
					curSubspaceLoad = curSubspaceLoad - remainingNodeLoad;
					remainingNodeLoad = 0.0;
				}
				else
				{
					remainingNodeLoad = remainingNodeLoad-curSubspaceLoad;
					curSubspaceLoad = 0.0;
				}
				
				nodeAssignmentList.add(nodeId);
				nodeAssignmentLoadMap.put(nodeId, remainingNodeLoad);
				
				if( curSubspaceLoad <= 0.0 )
				{
					break;
				}
			}
		}
		assert(nodeAssignmentList.size() > 0);
		return nodeAssignmentList;
	}
	
	private double getTotalLoadOnSystem()
	{
		double totalLoad = 0.0;
//		Iterator<Integer> nodeIter = loadMap.keySet().iterator();
//		while( nodeIter.hasNext() )
//		{
//			Integer nodeId = nodeIter.next();
//			double nodeLoad = loadMap.get(nodeId);
//			totalLoad = totalLoad + nodeLoad;
//		}
		return totalLoad;
	}
	
	
	private HashMap<Integer, Double> getOldSubspaceLoadMap()
	{
		HashMap<Integer, Double> subspaceLoadMap = new HashMap<Integer, Double>();
		
		Iterator<Integer> suspaceIdIter = oldSubspaceInfoMap.keySet().iterator();
		
		while( suspaceIdIter.hasNext() )
		{
			int subspaceId = suspaceIdIter.next();
			Vector<SubspaceInfo> replicasVect 
										= oldSubspaceInfoMap.get(subspaceId);
			
			// in basic configuration only one replica
			assert(replicasVect.size() == 1);
			Vector<Integer> subspaceNodes = replicasVect.get(0).getNodesOfSubspace();
			double totalSubspaceLoad = 0.0;
			
			for( int i=0; i<subspaceNodes.size(); i++ )
			{
				Integer nodeId = subspaceNodes.get(i);
//				double nodeLoad = loadMap.get(nodeId);
				double nodeLoad = 0.0;
				totalSubspaceLoad = totalSubspaceLoad + nodeLoad;
			}
			
			subspaceLoadMap.put(subspaceId, totalSubspaceLoad);
		}
		return subspaceLoadMap;
	}
	
	
	private void getOldConfiguration()
	{
		double numNodes = nodeConfig.getNodeIDs().size();
		double numAttrs = AttributeTypes.attributeMap.size();
		
		ContextServiceLogger.getLogger().fine("double divide "+numAttrs/optimalH+
				" numAttrs "+numAttrs+" optimalH "+optimalH);
		
		double numSubspaces = Math.ceil(numAttrs/optimalH);
		

		double numberNodesForSubspace = Math.floor(numNodes/numSubspaces);
		
		if( numberNodesForSubspace > 0 )
		{
			// first the basic nodes are assigned then remaining nodes are assigned 
			// uniformly to the existing subspaces.
			
			int nodesIdCounter 
				= assignNodesUniformlyToSubspaces
							(numberNodesForSubspace, (int)numSubspaces);
			
	
			Iterator<Integer> subspaceKeyIter = oldSubspaceInfoMap.keySet().iterator();
	
			while( nodesIdCounter < numNodes )
			{
				int subspaceKey = -1;
				
				if( subspaceKeyIter.hasNext() )
				{
					subspaceKey = subspaceKeyIter.next();
					Vector<SubspaceInfo> currSubVect
										= oldSubspaceInfoMap.get(subspaceKey);
					assert( currSubVect.size() > 0 );
					currSubVect.get(0).getNodesOfSubspace().add
								( (Integer)(Integer)nodesIdCounter );
					nodesIdCounter++;
				}
				else
				{
					subspaceKeyIter = oldSubspaceInfoMap.keySet().iterator();
				}
			}
		}
		else // numnodes are less than number of subspaces, so just assign each node to each subspace
		{
			// first the basic nodes are assigned then remaining nodes are assigned 
			// uniformly to the existing subspaces.
				
			int nodesIdCounter = 
				assignNodesUniformlyToSubspaces(numberNodesForSubspace, (int)numSubspaces);
			// above will not be able to assign any nodes
			assert(nodesIdCounter == 0);
			
			Iterator<Integer> subspaceKeyIter = oldSubspaceInfoMap.keySet().iterator();
	
			while( nodesIdCounter < numNodes )
			{
				int subspaceKey = -1;
				// assigning each node to each subspace
				while( subspaceKeyIter.hasNext() )
				{
					subspaceKey = subspaceKeyIter.next();
					Vector<SubspaceInfo> currSubVect
										= oldSubspaceInfoMap.get(subspaceKey);
					assert( currSubVect.size() > 0 );
					currSubVect.get(0).getNodesOfSubspace().add
								( (Integer)(Integer)nodesIdCounter );
				}
				nodesIdCounter++;
			}
		}
		// initializes the domain partitions for an attribute
		//initializePartitionInfo();
		initializeOldPartitionInfo();
		this.printSubspaceInfo();
	}
	
	private void initializeOldPartitionInfo()
	{
		Iterator<Integer> keyIter = oldSubspaceInfoMap.keySet().iterator();
		
		while( keyIter.hasNext() )
		{
			Vector<SubspaceInfo> currSubVect 
		 		= oldSubspaceInfoMap.get(keyIter.next());
			
			for(int i=0; i<currSubVect.size(); i++)
			{
				SubspaceInfo currSubInfo = currSubVect.get(i);
				int currSubspaceNumNodes = currSubInfo.getNodesOfSubspace().size();
				int currSubspaceNumAttrs = currSubInfo.getAttributesOfSubspace().size();
				
				int currSubspaceNumPartitions 
					= (int)Math.ceil(Math.pow(currSubspaceNumNodes, 
							1.0/currSubspaceNumAttrs));
				
				int currTriggerNumPartitions 
					= (int)Math.ceil
					(((double)currSubspaceNumNodes)/(double)currSubspaceNumAttrs);
				
				ContextServiceLogger.getLogger().fine("currSubspaceNumPartitions "
						+currSubspaceNumPartitions+" currTriggerNumPartitions "
						+currTriggerNumPartitions);
				
				assert(currTriggerNumPartitions > 0 );
				currSubInfo.setNumPartitions(currSubspaceNumPartitions);
				
				Vector<String> sortedAttrNameVect = new Vector<String>();
				Iterator<String> subspaceAttrIter
							= currSubInfo.getAttributesOfSubspace().keySet().iterator();
				
				while( subspaceAttrIter.hasNext() )
				{
					String attrName = subspaceAttrIter.next();
					sortedAttrNameVect.add(attrName);
				}
				sortedAttrNameVect.sort(null);
				
				//int currPartitionNum = 0;
				for(int j=0;j<sortedAttrNameVect.size();j++)
				{
					String attrName = sortedAttrNameVect.get(j);
					
					AttributePartitionInfo attrPartInfo 
						= currSubInfo.getAttributesOfSubspace().get(attrName);
						  attrPartInfo.initializePartitionInfo(currSubspaceNumPartitions, 
							currTriggerNumPartitions);
//					currPartitionNum++;
//					currPartitionNum= currPartitionNum%currSubspaceNumPartitions;
				}
			}
		}
	}
	
	private void loadAwareNodesAssignmentInASubspace()
	{
	}
	
//	private HashMap<Integer, Double> getLoadOfASubspace( 
//				List<PartitionToNodeInfo<Integer>> subspacePartitionList )
//	{
//		
//		
//	}
	
	/**
	 * This function assigns nodes uniformly to subspaces
	 * and stores them in the inherited subspaceInfoVector 
	 * vector. returns the nodeId of the first unassigned node
	 * nodeId <= numNodes
	 */
	private int assignNodesUniformlyToSubspaces
			( double numberNodesForSubspace, int numSubspaces )
	{
		Vector<AttributeMetaInfo> attrVector = new Vector<AttributeMetaInfo>();
		attrVector.addAll(AttributeTypes.attributeMap.values());
		
		
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
				= new SubspaceInfo
				  ( distinctSubspaceId, 0, subspaceAttrs, subspaceNodes );
			
			Vector<SubspaceInfo> replicatedSubspacesVector 
						= new Vector<SubspaceInfo>(); 
			replicatedSubspacesVector.add(currSubInfo);
			
			oldSubspaceInfoMap.put(distinctSubspaceId, replicatedSubspacesVector);
		}
		//all attributes should be assigned to some subspace by end of this function.
		assert(attrIndexCounter == attrVector.size() );
		return nodesIdCounter;
	}
}