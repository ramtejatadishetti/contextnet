package edu.umass.cs.contextservice.configurator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;

import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.HyperspaceDB;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.nio.interfaces.NodeConfig;

public abstract class AbstractSubspaceConfigurator
{
	protected NodeConfig<Integer> nodeConfig;
	
	// stores subspace info
	// key is the distinct subspace id and it stores all replicas of that subspace.
	// a replica of a subspace is defined over same attributes but different nodes
	// this map is written only once, in one thread,  and read many times, by many threads, 
	// so no need to make concurrent.
	protected  HashMap<Integer, Vector<SubspaceInfo>> subspaceInfoMap;
	
	private final Object subspacePartitionInsertLock				= new Object();
	
	// this can be a huge number, it is exponential in numebr of attributes.
	private long subspacePartitionInsertSent						= 0;
	private long subspacePartitionInsertCompl						= 0;
	
	public AbstractSubspaceConfigurator(NodeConfig<Integer> nodeConfig)
	{
		this.nodeConfig = nodeConfig;
		subspaceInfoMap = new HashMap<Integer, Vector<SubspaceInfo>>();
	}
	
	public abstract void configureSubspaceInfo();
	
	protected void printSubspaceInfo()
	{
		Iterator<Integer> subspceIter = subspaceInfoMap.keySet().iterator();
		
		while( subspceIter.hasNext() )
		{
			int distinctSubId = subspceIter.next();
			
			Vector<SubspaceInfo> replicaVect = subspaceInfoMap.get(distinctSubId);
			ContextServiceLogger.getLogger().fine("number of replicas for subspaceid "+distinctSubId
					+" "+replicaVect.size());
			for(int i=0; i<replicaVect.size();i++)
			{
				SubspaceInfo currSubspace = replicaVect.get(i);
				ContextServiceLogger.getLogger().fine(currSubspace.toString());
			}
		}
	}
	
	public HashMap<Integer, Vector<SubspaceInfo>> getSubspaceInfoMap()
	{
		return this.subspaceInfoMap;
	}
	
	/**
	 * recursive function to generate all the
	 * subspace regions/partitions.
	 */
	public void generateAndStoreSubspacePartitionsInDB(ExecutorService nodeES, 
			HyperspaceDB hyperspaceDB )
	{
		ContextServiceLogger.getLogger().fine
								(" generateSubspacePartitions() entering " );
		
		Iterator<Integer> subspaceIter = subspaceInfoMap.keySet().iterator();
		
		while( subspaceIter.hasNext() )
		{
			int subspaceId = subspaceIter.next();
			Vector<SubspaceInfo> replicaVect 
								= subspaceInfoMap.get(subspaceId);
			
			for( int i=0; i<replicaVect.size(); i++ )
			{
				SubspaceInfo subspaceInfo = replicaVect.get(i);
				HashMap<String, AttributePartitionInfo> attrsOfSubspace 
										= subspaceInfo.getAttributesOfSubspace();
				
				Vector<Integer> nodesOfSubspace = subspaceInfo.getNodesOfSubspace();
				
				double numAttr  = attrsOfSubspace.size();
				//double numNodes = nodesOfSubspace.size();
				ContextServiceLogger.getLogger().fine(" NumPartitions "
												+subspaceInfo.getNumPartitions() );
				
				Integer[] partitionNumArray = new Integer[subspaceInfo.getNumPartitions()];
				for(int j = 0; j<partitionNumArray.length; j++)
				{
					partitionNumArray[j] = new Integer(j);
				}
				
				// Create the initial vector of 2 elements (apple, orange)
				ICombinatoricsVector<Integer> originalVector 
											= Factory.createVector(partitionNumArray);

				// Create the generator by calling the appropriate method in the Factory class. 
				// Set the second parameter as 3, since we will generate 3-elemets permutations
				Generator<Integer> gen 
					= Factory.createPermutationWithRepetitionGenerator(originalVector, (int)numAttr);
				
				// Print the result
				int nodeIdCounter = 0;
				int sizeOfNumNodes = nodesOfSubspace.size();
				List<List<Integer>> subspaceVectList = new LinkedList<List<Integer>>();
				List<Integer> respNodeIdList = new LinkedList<Integer>();
				long counter = 0;
				for( ICombinatoricsVector<Integer> perm : gen )
				{
					Integer respNodeId = nodesOfSubspace.get(nodeIdCounter%sizeOfNumNodes);
					//ContextServiceLogger.getLogger().fine("perm.getVector() "+perm.getVector());
					counter++;
					
					if(counter % ContextServiceConfig.SUBSPACE_PARTITION_INSERT_BATCH_SIZE == 0)
					{
						subspaceVectList.add(perm.getVector());
						respNodeIdList.add(respNodeId);
						
						synchronized(this.subspacePartitionInsertLock)
						{
							this.subspacePartitionInsertSent++;
						}
						
						DatabaseOperationClass dbOper = new DatabaseOperationClass(subspaceInfo.getSubspaceId(), subspaceInfo.getReplicaNum(), 
								subspaceVectList, respNodeIdList, hyperspaceDB);
						//dbOper.run();
						
						nodeES.execute(dbOper);
						
						// repointing it to a new list, and the pointer to the old list is passed to the DatabaseOperation class
						subspaceVectList = new LinkedList<List<Integer>>();
						respNodeIdList = new LinkedList<Integer>();
						
						
						nodeIdCounter++;
					}
					else
					{
						subspaceVectList.add(perm.getVector());
						respNodeIdList.add(respNodeId);
						nodeIdCounter++;
					}
				}
				// adding the remaning ones
				if(subspaceVectList.size() > 0)
				{
					synchronized(this.subspacePartitionInsertLock)
					{
						this.subspacePartitionInsertSent++;
					}
					
					DatabaseOperationClass dbOper = new DatabaseOperationClass(subspaceInfo.getSubspaceId(), subspaceInfo.getReplicaNum(), 
							subspaceVectList, respNodeIdList, hyperspaceDB);
					
					nodeES.execute(dbOper);
					
					// repointing it to a new list, and the pointer to the old list is passed to the DatabaseOperation class
					subspaceVectList = new LinkedList<List<Integer>>();
					respNodeIdList = new LinkedList<Integer>();
				}
			}
		}
		
		synchronized(this.subspacePartitionInsertLock)
		{
			while(this.subspacePartitionInsertSent != this.subspacePartitionInsertCompl)
			{
				try 
				{
					this.subspacePartitionInsertLock.wait();
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
		}
		ContextServiceLogger.getLogger().fine
							(" generateSubspacePartitions() completed " );
	}
	
	protected void initializePartitionInfo()
	{
		Iterator<Integer> keyIter = subspaceInfoMap.keySet().iterator();
		
		while( keyIter.hasNext() )
		{
			Vector<SubspaceInfo> currSubVect 
		 		= subspaceInfoMap.get(keyIter.next());
			
			for(int i=0; i<currSubVect.size(); i++)
			{
				SubspaceInfo currSubInfo = currSubVect.get(i);
				int currSubspaceNumNodes = currSubInfo.getNodesOfSubspace().size();
				int currSubspaceNumAttrs = currSubInfo.getAttributesOfSubspace().size();
				
				//int currSubspaceNumPartitions 
				//	= (int)Math.ceil(Math.pow(currSubspaceNumNodes, 1.0/currSubspaceNumAttrs));
				int currSubspaceNumPartitions =  -1;
				if(this.getClass() == BasicSubspaceConfigurator.class)
				{
					currSubspaceNumPartitions = getNumberOfPartitionsUsingUniformHeuristics(
							currSubspaceNumNodes, currSubspaceNumAttrs);
				}
				else if(this.getClass() == ReplicatedSubspaceConfigurator.class)
				{
					// there are many subsapces replication, so we go with the the minimum partition requireed.
					currSubspaceNumPartitions 
						= (int)Math.ceil(Math.pow(currSubspaceNumNodes, 1.0/currSubspaceNumAttrs));
				}
				
				
				System.out.println("Num of partitions "+currSubspaceNumPartitions 
						+" repnum "+currSubInfo.getReplicaNum());
				//FIXME: change the uniform partition for triggers too.
				int currTriggerNumPartitions 
					= (int)Math.ceil(((double)currSubspaceNumNodes)/(double)currSubspaceNumAttrs);
				
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
	
	
	private static int getNumberOfPartitionsUsingUniformHeuristics(
			int currSubspaceNumNodes, int currSubspaceNumAttrs)
	{
		int currSubspaceNumPartitions 
			= (int)Math.ceil(Math.pow(currSubspaceNumNodes, 1.0/currSubspaceNumAttrs));
		
		// we only check for next 10 partitions.
		int currpart = currSubspaceNumPartitions;
		
		int maxPartNum = -1;
		double maxJFI = -1;
		
		while(currpart < (currSubspaceNumPartitions+ContextServiceConfig.NUM_PARTITITON_LOOK_AHEAD))
		{
			double numRegions = Math.pow(currpart, currSubspaceNumAttrs);
			
			if(numRegions>= ContextServiceConfig.MAX_REGION_LOOK_AHEAD)
			{
				if(maxPartNum == -1)
				{
					maxPartNum = currSubspaceNumPartitions;
				}
				break;
			}
			
			double div = Math.floor(numRegions/currSubspaceNumNodes);
			
			double[] nodeArray = new double[currSubspaceNumNodes];
			
			for(int i=0; i<currSubspaceNumNodes; i++)
			{
				nodeArray[i] = div;
			}
			
			double rem = Math.floor(numRegions%currSubspaceNumNodes);
			
			for(int i=0; i<rem; i++)
			{
				nodeArray[i] = nodeArray[i] + 1;
			}
			
			double jfi = computeJainsFairnessIndex(nodeArray);
			
			if(maxPartNum == -1)
			{
				maxJFI = jfi;
				maxPartNum = currpart;
				//System.out.println("max jfi "+maxJFI+" part "+maxPartNum);
			}
			else
			{
				if(jfi > maxJFI)
				{
					maxJFI = jfi;
					maxPartNum = currpart;
					//System.out.println("max jfi "+maxJFI+" part "+maxPartNum);
				}
			}
			
			currpart = currpart + 1;
		}	
		
		return maxPartNum;
	}
	
	
	private int getNumberOfPartitionsUsingJustGreaterHeuristics(int currSubspaceNumNodes
			, int currSubspaceNumAttrs)
	{
		int currSubspaceNumPartitions 
			= (int)Math.ceil(Math.pow(currSubspaceNumNodes, 1.0/currSubspaceNumAttrs));
		
		return currSubspaceNumPartitions;
	}
	
	
	private static double computeJainsFairnessIndex(double[] listOfNumbers)
	{
		double sum = 0.0;
		for(int i=0; i<listOfNumbers.length; i++)
		{
			sum = sum + listOfNumbers[i];
		}
		
		double squareSum = 0.0;
		for(int i=0; i < listOfNumbers.length; i++)
		{
			squareSum = squareSum + Math.pow(listOfNumbers[i], 2);
		}
		
		double jfi = Math.pow(sum, 2)/(listOfNumbers.length * squareSum);
		return jfi;
	}
	
	
	private class DatabaseOperationClass implements Runnable
	{
		private final int subspaceId;
		private final int replicaNum;
		private final List<List<Integer>> permVectorList;
		private final List<Integer> respNodeIdList;
		private final HyperspaceDB hyperspaceDB;
		
		public DatabaseOperationClass(int subspaceId, int replicaNum, 
				List<List<Integer>> permVectorList
				, List<Integer> respNodeIdList,
				HyperspaceDB hyperspaceDB )
		{
			this.subspaceId = subspaceId;
			this.replicaNum = replicaNum;
			this.permVectorList = permVectorList;
			this.respNodeIdList = respNodeIdList;
			this.hyperspaceDB = hyperspaceDB;
		}
		
		@Override
		public void run() 
		{
			try
			{
				hyperspaceDB.bulkInsertIntoSubspacePartitionInfo(subspaceId, replicaNum, 
						permVectorList, respNodeIdList);
				synchronized(subspacePartitionInsertLock)
				{
					subspacePartitionInsertCompl++;
					if(subspacePartitionInsertCompl == subspacePartitionInsertSent)
					{
						subspacePartitionInsertLock.notify();
					}
				}
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			catch(Error ex)
			{
				ex.printStackTrace();
			}
		}
	}
	
	
	public static void main(String[] args)
	{
		// testing getNumberOfPartitionsUsingUniformHeuristics(
		//int currSubspaceNumNodes, int currSubspaceNumAttrs);
		
		int numParts = getNumberOfPartitionsUsingUniformHeuristics(
				11, 3);
		
		System.out.println("numParts "+numParts);
	}
}