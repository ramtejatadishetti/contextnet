package edu.umass.cs.contextservice.configurator;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;

import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes.DomainPartitionInfo;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.datasource.AbstractDataSource;
import edu.umass.cs.contextservice.database.datasource.AbstractDataSource.DB_REQUEST_TYPE;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.utils.DelayProfiler;

public abstract class AbstractSubspaceConfigurator
{
	protected NodeConfig<Integer> nodeConfig;
	
	// stores subspace info
	// key is the distinct subspace id and it stores all replicas of that subspace.
	// a replica of a subspace is defined over same attributes but different nodes
	// this map is written only once, in one thread,  and read many times, by many threads, 
	// so no need to make concurrent.
	protected  HashMap<Integer, Vector<SubspaceInfo>> subspaceInfoMap;
	protected final AbstractDataSource dataSource;
	
	private final Object subspacePartitionInsertLock				= new Object();
	
	// this can be a huge number, it is exponential in number of attributes.
	private long subspacePartitionInsertSent						= 0;
	private long subspacePartitionInsertCompl						= 0;
	
	
	public AbstractSubspaceConfigurator(NodeConfig<Integer> nodeConfig, 
						AbstractDataSource dataSource)
	{
		this.nodeConfig = nodeConfig;
		subspaceInfoMap = new HashMap<Integer, Vector<SubspaceInfo>>();
		this.dataSource = dataSource;
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
	public void generateAndStoreSubspacePartitionsInDB(ExecutorService nodeES)
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
								subspaceVectList, respNodeIdList);
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
				if( subspaceVectList.size() > 0 )
				{
					synchronized(this.subspacePartitionInsertLock)
					{
						this.subspacePartitionInsertSent++;
					}
					
					DatabaseOperationClass dbOper = new DatabaseOperationClass(subspaceInfo.getSubspaceId(), subspaceInfo.getReplicaNum(), 
							subspaceVectList, respNodeIdList);
					
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

				
				int currSubspaceNumPartitions 
						= (int)Math.ceil(
						Math.pow(currSubspaceNumNodes, 1.0/currSubspaceNumAttrs));
				
				
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
	
	
	/**
	 * Bulk insert is needed when number of partitions are very large.
	 * Not using prepstmt, multiple inserts in single insert is faster
	 * @param subspaceId
	 * @param replicaNum
	 * @param subspaceVectorList
	 * @param respNodeIdList
	 */
	public void bulkInsertIntoSubspacePartitionInfo( int subspaceId, int replicaNum,
			List<List<Integer>> subspaceVectorList, List<Integer> respNodeIdList )
	{
		assert(subspaceVectorList.size() == respNodeIdList.size());
		
		ContextServiceLogger.getLogger().fine("bulkInsertIntoSubspacePartitionInfo called subspaceId "
				+subspaceId + " replicaNum "+replicaNum+" "+subspaceVectorList.size()+" "
				+respNodeIdList.size() );
		
		long t0 							= System.currentTimeMillis();
		Connection myConn   				= null;
		Statement stmt      				= null;
		
		String tableName = "subspaceId"+subspaceId+"RepNum"+replicaNum+"PartitionInfo";
		
		SubspaceInfo currSubInfo = subspaceInfoMap.
				get(subspaceId).get(replicaNum);
		//Vector<AttributePartitionInfo> domainPartInfo = currSubInfo.getDomainPartitionInfo();
		//Vector<String> attrSubspaceInfo = currSubInfo.getAttributesOfSubspace();
		HashMap<String, AttributePartitionInfo> attrSubspaceInfo = currSubInfo.getAttributesOfSubspace();
		
		// subspace vector denotes parition num for each attribute 
		// in this subspace and attrSubspaceInfo.size denotes total 
		// number of attributes. The size of both should be same
		// as both denote number of attributes in this subspace.
		if(attrSubspaceInfo.size() != subspaceVectorList.get(0).size())
		{
			assert(false);
		}
		
		String insertTableSQL = " INSERT INTO "+tableName 
				+" ( hashCode, respNodeID ";
				//+ "nodeID) " + "VALUES"
				//+ "("+lowerRange+","+upperRange+","+nodeID +")";
		
		Iterator<String> attrIter = attrSubspaceInfo.keySet().iterator();
		while(attrIter.hasNext())
		{
			String attrName = attrIter.next();
			
			String lowerAtt = "lower"+attrName;
			String upperAtt = "upper"+attrName;
			
			insertTableSQL = insertTableSQL + ", "+lowerAtt+" , "+upperAtt;
		}
		insertTableSQL = insertTableSQL + " ) VALUES ";
		
		
		for( int i=0; i<subspaceVectorList.size(); i++ )
		{
			List<Integer> subspaceVector = subspaceVectorList.get(i);
			Integer respNodeId = respNodeIdList.get(i);
			
			if(i == 0)
			{
				insertTableSQL = insertTableSQL +" ( "+subspaceVector.hashCode()+" , "+
					Integer.parseInt(respNodeId.toString());
			}
			else
			{
				insertTableSQL = insertTableSQL +" , ( "+subspaceVector.hashCode()+" , "+
						Integer.parseInt(respNodeId.toString());
			}
			
			attrIter = attrSubspaceInfo.keySet().iterator();
			int counter =0;
			while(attrIter.hasNext())
			{
				String attrName = attrIter.next();
				AttributePartitionInfo attrPartInfo = attrSubspaceInfo.get(attrName);
				int partitionNum = subspaceVector.get(counter);
				DomainPartitionInfo domainPartInfo 
					= attrPartInfo.getSubspaceDomainPartitionInfo().get(partitionNum);
				// if it is a String then single quotes needs to be added
				
				AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
				String dataType = attrMetaInfo.getDataType();
				
				
				String lowerBound  = AttributeTypes.convertStringToDataTypeForMySQL
								(domainPartInfo.lowerbound, dataType)+"";
				String upperBound  = AttributeTypes.convertStringToDataTypeForMySQL
								(domainPartInfo.upperbound, dataType)+"";
				
				insertTableSQL = insertTableSQL + " , "+lowerBound+" , "+ 
						upperBound;
				counter++;
			}
			insertTableSQL = insertTableSQL +" ) ";
		}
		
		
		try
		{
			myConn = dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
			stmt = myConn.createStatement();
			// execute insert SQL statement
			stmt.executeUpdate(insertTableSQL);
		} catch(SQLException sqlex)
		{
			sqlex.printStackTrace();
		}
		finally
		{
			try
			{
				if( myConn != null )
				{
					myConn.close();
				}
				if( stmt != null )
				{
					stmt.close();
				}
			} catch(SQLException sqex)
			{
				sqex.printStackTrace();
			}
		}
		
		ContextServiceLogger.getLogger().fine("bulkInsertIntoSubspacePartitionInfo completed "
				+ subspaceVectorList.size()+" "+respNodeIdList.size() );
		
		if( ContextServiceConfig.DELAY_PROFILER_ON )
		{
			DelayProfiler.updateDelay("insertIntoSubspacePartitionInfo", t0);
		}
	}
	
	
	/**
	 * Inserts a subspace region denoted by subspace vector, 
	 * integer denotes partition num in partition info 
	 * @param subspaceNum
	 * @param subspaceVector
	 */
	public void insertIntoSubspacePartitionInfo(int subspaceId, int replicaNum,
			List<Integer> subspaceVector, Integer respNodeId)
	{
		long t0 			= System.currentTimeMillis();
		Connection myConn   = null;
		Statement stmt      = null;
		
		String tableName = "subspaceId"+subspaceId+"RepNum"+replicaNum+"PartitionInfo";
		
		SubspaceInfo currSubInfo = subspaceInfoMap.
				get(subspaceId).get(replicaNum);
		//Vector<AttributePartitionInfo> domainPartInfo = currSubInfo.getDomainPartitionInfo();
		//Vector<String> attrSubspaceInfo = currSubInfo.getAttributesOfSubspace();
		HashMap<String, AttributePartitionInfo> attrSubspaceInfo = currSubInfo.getAttributesOfSubspace();
		
		// subspace vector denotes parition num for each attribute 
		// in this subspace and attrSubspaceInfo.size denotes total 
		// number of attributes. The size of both should be same
		// as both denote number of attributes in this subspace.
		if(attrSubspaceInfo.size() != subspaceVector.size())
		{
			assert(false);
		}
		
		String insertTableSQL = "SET unique_checks=0; INSERT INTO "+tableName 
				+" ( hashCode, respNodeID ";
				//+ "nodeID) " + "VALUES"
				//+ "("+lowerRange+","+upperRange+","+nodeID +")";
		
		Iterator<String> attrIter = attrSubspaceInfo.keySet().iterator();
		while(attrIter.hasNext())
		{
			String attrName = attrIter.next();
			
			String lowerAtt = "lower"+attrName;
			String upperAtt = "upper"+attrName;
			
			insertTableSQL = insertTableSQL + ", "+lowerAtt+" , "+upperAtt;
		}
		
		insertTableSQL = insertTableSQL + " ) VALUES ( "+subspaceVector.hashCode() + 
				" , "+respNodeId;
		
		attrIter = attrSubspaceInfo.keySet().iterator();
		int counter =0;
		while(attrIter.hasNext())
		{
			String attrName = attrIter.next();
			AttributePartitionInfo attrPartInfo = attrSubspaceInfo.get(attrName);
			int partitionNum = subspaceVector.get(counter);
			DomainPartitionInfo domainPartInfo = attrPartInfo.getSubspaceDomainPartitionInfo().get(partitionNum);
			// if it is a String then single quotes needs to be added
			
			AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
			String dataType = attrMetaInfo.getDataType();
			
			String lowerBound  = AttributeTypes.convertStringToDataTypeForMySQL(domainPartInfo.lowerbound, dataType)+"";
			String upperBound  = AttributeTypes.convertStringToDataTypeForMySQL(domainPartInfo.upperbound, dataType)+"";
			
			insertTableSQL = insertTableSQL + " , "+lowerBound+" , "+ 
					upperBound;
			
			counter++;
		}
		
		insertTableSQL = insertTableSQL + " ) ";
		
		try
		{
			myConn = this.dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
			stmt = myConn.createStatement();

			// execute insert SQL stetement
			stmt.executeUpdate(insertTableSQL);
			
		} catch(SQLException sqlex)
		{
			sqlex.printStackTrace();
		}
		finally
		{
			try
			{
				if( myConn != null )
				{
					myConn.close();
				}
				if( stmt != null )
				{
					stmt.close();
				}
			} catch(SQLException sqex)
			{
				sqex.printStackTrace();
			}
		}
		
		if( ContextServiceConfig.DELAY_PROFILER_ON )
		{
			DelayProfiler.updateDelay("insertIntoSubspacePartitionInfo", t0);
		}
	}
	
	private class DatabaseOperationClass implements Runnable
	{
		private final int subspaceId;
		private final int replicaNum;
		private final List<List<Integer>> permVectorList;
		private final List<Integer> respNodeIdList;
		
		
		public DatabaseOperationClass(int subspaceId, int replicaNum, 
				List<List<Integer>> permVectorList
				, List<Integer> respNodeIdList)
		{
			this.subspaceId = subspaceId;
			this.replicaNum = replicaNum;
			this.permVectorList = permVectorList;
			this.respNodeIdList = respNodeIdList;

		}
		
		@Override
		public void run() 
		{
			try
			{
				bulkInsertIntoSubspacePartitionInfo(subspaceId, replicaNum, 
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
	}
}



//private static int getNumberOfPartitionsUsingUniformHeuristics(
//int currSubspaceNumNodes, int currSubspaceNumAttrs)
//{
//int currSubspaceNumPartitions 
//= (int)Math.ceil(Math.pow(currSubspaceNumNodes, 1.0/currSubspaceNumAttrs));
//
//// we only check for next 10 partitions.
//int currpart = currSubspaceNumPartitions;
//
//int maxPartNum = -1;
//double maxJFI = -1;
//
//while(currpart < (currSubspaceNumPartitions+ContextServiceConfig.NUM_PARTITITON_LOOK_AHEAD))
//{
//double numRegions = Math.pow(currpart, currSubspaceNumAttrs);
//
//if(numRegions>= ContextServiceConfig.MAX_REGION_LOOK_AHEAD)
//{
//	if(maxPartNum == -1)
//	{
//		maxPartNum = currSubspaceNumPartitions;
//	}
//	break;
//}
//
//double div = Math.floor(numRegions/currSubspaceNumNodes);
//
//List<Double> nodeList = new LinkedList<Double>();;
//
//for(int i=0; i<currSubspaceNumNodes; i++)
//{
//	nodeList.add(div);
//	nodeArray[i] = div;
//}
//
//double rem = Math.floor(numRegions%currSubspaceNumNodes);
//
//for(int i=0; i<rem; i++)
//{
//	nodeArray[i] = nodeArray[i] + 1;
//}
//
//double jfi = computeJainsFairnessIndex(nodeArray);
//
//if(maxPartNum == -1)
//{
//	maxJFI = jfi;
//	maxPartNum = currpart;
//	//System.out.println("max jfi "+maxJFI+" part "+maxPartNum);
//}
//else
//{
//	if(jfi > maxJFI)
//	{
//		maxJFI = jfi;
//		maxPartNum = currpart;
//		//System.out.println("max jfi "+maxJFI+" part "+maxPartNum);
//	}
//}
//
//currpart = currpart + 1;
//}	
//
//return maxPartNum;
//}


//private int getNumberOfPartitionsUsingJustGreaterHeuristics(int currSubspaceNumNodes
//, int currSubspaceNumAttrs)
//{
//int currSubspaceNumPartitions 
//= (int)Math.ceil(Math.pow(currSubspaceNumNodes, 1.0/currSubspaceNumAttrs));
//
//return currSubspaceNumPartitions;
//}