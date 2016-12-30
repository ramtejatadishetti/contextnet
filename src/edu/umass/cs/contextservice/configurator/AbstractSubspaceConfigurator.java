package edu.umass.cs.contextservice.configurator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;


import edu.umass.cs.contextservice.attributeInfo.AttributeTypes.RangePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.regionmapper.helper.RegionInfo;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;
import edu.umass.cs.nio.interfaces.NodeConfig;


public abstract class AbstractSubspaceConfigurator
{
	protected NodeConfig<Integer> nodeConfig;
	
	// stores subspace info
	// key is the distinct subspace id and it stores all replicas of that subspace.
	// a replica of a subspace is defined over same attributes but different nodes
	// this map is written only once, in one thread,  and read many times, by many threads, 
	// so no need to make concurrent.
	protected  HashMap<Integer, List<SubspaceInfo>> subspaceInfoMap;

	
	public AbstractSubspaceConfigurator(NodeConfig<Integer> nodeConfig)
	{
		this.nodeConfig = nodeConfig;
		subspaceInfoMap = new HashMap<Integer, List<SubspaceInfo>>();
	}
	
	public abstract void configureSubspaceInfo();
	
	protected void printSubspaceInfo()
	{
		Iterator<Integer> subspceIter = subspaceInfoMap.keySet().iterator();
		
		while( subspceIter.hasNext() )
		{
			int distinctSubId = subspceIter.next();
			
			List<SubspaceInfo> replicaVect = subspaceInfoMap.get(distinctSubId);
			ContextServiceLogger.getLogger().fine("number of replicas for subspaceid "+distinctSubId
					+" "+replicaVect.size());
			for(int i=0; i<replicaVect.size();i++)
			{
				SubspaceInfo currSubspace = replicaVect.get(i);
				ContextServiceLogger.getLogger().fine(currSubspace.toString());
			}
		}
	}
	
	public HashMap<Integer, List<SubspaceInfo>> getSubspaceInfoMap()
	{
		return this.subspaceInfoMap;
	}
	
	protected void initializePartitionInfo()
	{
		Iterator<Integer> keyIter = subspaceInfoMap.keySet().iterator();
		
		while( keyIter.hasNext() )
		{
			List<SubspaceInfo> currSubVect 
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
						  attrPartInfo.initializePartitionInfo(currSubspaceNumPartitions);
				}
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


/**
 * Bulk insert is needed when number of partitions are very large.
 * Not using prepstmt, multiple inserts in single insert is faster
 * @param subspaceId
 * @param replicaNum
 * @param subspaceVectorList
 * @param respNodeIdList
 */
/*public void bulkInsertIntoSubspacePartitionInfo( int subspaceId, int replicaNum,
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
}*/