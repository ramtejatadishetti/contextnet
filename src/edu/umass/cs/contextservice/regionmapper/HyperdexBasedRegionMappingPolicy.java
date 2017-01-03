package edu.umass.cs.contextservice.regionmapper;

import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes.RangePartitionInfo;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.configurator.AbstractSubspaceConfigurator;
import edu.umass.cs.contextservice.configurator.BasicSubspaceConfigurator;
import edu.umass.cs.contextservice.database.datasource.AbstractDataSource;
import edu.umass.cs.contextservice.database.datasource.SQLiteDataSource;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.queryparsing.QueryParser;
import edu.umass.cs.contextservice.regionmapper.database.AbstractRegionMappingStorage;
import edu.umass.cs.contextservice.regionmapper.database.HyperdexSQLRegionMappingStorage;
import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;
import edu.umass.cs.contextservice.regionmapper.helper.RegionInfo;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;



public class HyperdexBasedRegionMappingPolicy extends AbstractRegionMappingPolicy
{
	private final AbstractSubspaceConfigurator subspaceConfigurator;
	private final AbstractRegionMappingStorage regionMappingStorage;
	
	
	public HyperdexBasedRegionMappingPolicy( HashMap<String, AttributeMetaInfo> attributeMap, 
			CSNodeConfig csNodeConfig, int numberAttrsPerSubspace, AbstractDataSource dataSource )
	{
		super(attributeMap, csNodeConfig);
		
		subspaceConfigurator 
			= new BasicSubspaceConfigurator(csNodeConfig, numberAttrsPerSubspace);
		
		regionMappingStorage = new HyperdexSQLRegionMappingStorage(dataSource, 
				subspaceConfigurator.getSubspaceInfoMap() );
	}
	

	@Override
	public void computeRegionMapping() 
	{
		subspaceConfigurator.configureSubspaceInfo();
//		HashMap<Integer, Vector<SubspaceInfo>> subspaceInfoMap 
//					= subspaceConfigurator.getSubspaceInfoMap();
		regionMappingStorage.createTables();
		generateAndStoreSubspaceRegions();	
		
		// printing subsapces.
		HashMap<Integer, List<SubspaceInfo>> subspaceMap = subspaceConfigurator.getSubspaceInfoMap();
		
		Iterator<Integer> idIter = subspaceMap.keySet().iterator();
		
		while(idIter.hasNext())
		{
			int subspaceId = idIter.next();
			SubspaceInfo subsInfo = subspaceMap.get(subspaceId).get(0);
			
			System.out.println(subsInfo.toString());
		}
	}
	
	
	public List<Integer> getNodeIDsForAValueSpaceForUpdate
			(String GUID, ValueSpaceInfo valueSpace)
	{
		HashMap<Integer, Boolean> nodeMap = new HashMap<Integer, Boolean>();
		
		
		HashMap<Integer, List<SubspaceInfo>> subspaceMap = 
						subspaceConfigurator.getSubspaceInfoMap();
		
		Iterator<Integer> subspaceIdIter = subspaceMap.keySet().iterator();
		
		while( subspaceIdIter.hasNext() )
		{
			int subspaceId = subspaceIdIter.next();
			
			SubspaceInfo subInfo = subspaceMap.get(subspaceId).get(0);		
			
			String tableName = "subspaceId"+subspaceId+"RepNum0PartitionInfo";
			
			ValueSpaceInfo updateSubspaceValSpace 	= new ValueSpaceInfo();
			
			HashMap<String, AttributePartitionInfo> subspaceAttrMap 
													= subInfo.getAttributesOfSubspace();
			
			Iterator<String> attrIter = subspaceAttrMap.keySet().iterator();
			
			while( attrIter.hasNext() )
			{
				String attrName = attrIter.next();
				updateSubspaceValSpace.getValueSpaceBoundary().put(attrName, 
										valueSpace.getValueSpaceBoundary().get(attrName));
			}
			
			List<Integer> list = 
				regionMappingStorage.getNodeIdsForValueSpace(tableName, updateSubspaceValSpace).get(0);
			
			
			for(int i=0; i<list.size(); i++)
			{
				nodeMap.put(list.get(i), true);
			}
		}
		
		List<Integer> nodeList = new LinkedList<Integer>();
		Iterator<Integer> nodeIdIter = nodeMap.keySet().iterator();
		while(nodeIdIter.hasNext())
		{
			nodeList.add(nodeIdIter.next());
		}
		return nodeList;
	}
	
	
	public List<Integer> getNodeIDsForAValueSpaceForSearch
									(ValueSpaceInfo valueSpace)
	{
		HashMap<String, AttributeValueRange> queryAttrMap 
								= getNotFullRangeAttrsInSearchQuery(valueSpace);
		
		SubspaceInfo subInfo 	= getMaxOverlapSubspace(queryAttrMap );
		
		//System.out.println("Max sub info "+subInfo.toString());
		// Query value space consisting only subspace attrs.
		
		ValueSpaceInfo searchSubspaceValSpace 	= new ValueSpaceInfo();
		
		HashMap<String, AttributePartitionInfo> subspaceAttrMap 
												= subInfo.getAttributesOfSubspace();
		
		Iterator<String> attrIter = subspaceAttrMap.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			searchSubspaceValSpace.getValueSpaceBoundary().put(attrName, 
									valueSpace.getValueSpaceBoundary().get(attrName));
		}
		
		
		HashMap<Integer, Boolean> nodeMap = new HashMap<Integer, Boolean>();
		
		String tableName = "subspaceId"+subInfo.getSubspaceId()+"RepNum0PartitionInfo";
		
		List<Integer> list = 
				regionMappingStorage.getNodeIdsForValueSpace(tableName, searchSubspaceValSpace).get(0);
		
		for(int i=0; i<list.size(); i++)
		{
			nodeMap.put(list.get(i), true);
		}
		
		
		List<Integer> nodeList = new LinkedList<Integer>();
		Iterator<Integer> nodeIdIter = nodeMap.keySet().iterator();
		while(nodeIdIter.hasNext())
		{
			nodeList.add(nodeIdIter.next());
		}
		return nodeList;
	}
	
	private HashMap<String, AttributeValueRange> 
					getNotFullRangeAttrsInSearchQuery(ValueSpaceInfo valueSpace)
	{
		HashMap<String, AttributeValueRange> valSpaceBoundary 
												= valueSpace.getValueSpaceBoundary();
		
		Iterator<String> attrIter 				= valSpaceBoundary.keySet().iterator();
		
		HashMap<String, AttributeValueRange> queryAttrMap 
												= new HashMap<String, AttributeValueRange>();
		
		while(attrIter.hasNext())
		{
			String attrName = attrIter.next();
			AttributeValueRange attrValRange = valSpaceBoundary.get(attrName);
			
			AttributeMetaInfo attrMetaInfo = attributeMap.get(attrName);
			
			if( attrValRange.getLowerBound().equals(attrMetaInfo.getMinValue()) 
					&& attrValRange.getUpperBound().equals(attrMetaInfo.getMaxValue()) )
			{
				// non interesting attr.
			}
			else
			{
				queryAttrMap.put(attrName, attrValRange);
			}
		}
		return queryAttrMap;
	}
	
	
	/**
	 * Returns subspace number of the maximum overlapping
	 * subspace. Used in processing search query.
	 * @return
	 */
	private SubspaceInfo getMaxOverlapSubspace( HashMap<String, AttributeValueRange> 
												searchAttrRange)
	{
		// first the maximum matching subspace is found and then any of its replica it chosen
		Iterator<Integer> keyIter   	= subspaceConfigurator.getSubspaceInfoMap().keySet().iterator();
		int maxMatchingAttrs 			= 0;
		
		HashMap<Integer, List<MaxAttrMatchingStorageClass>> matchingSubspaceHashMap = 
				new HashMap<Integer, List<MaxAttrMatchingStorageClass>>();
		
		while( keyIter.hasNext() )
		{
			int subspaceId = keyIter.next();
			SubspaceInfo currSubInfo 
						= subspaceConfigurator.getSubspaceInfoMap().get(subspaceId).get(0);
			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo 
						= currSubInfo.getAttributesOfSubspace();
			
			int currMaxMatch = 0;
			List<String> currMatchingAttrList = new LinkedList<String>();
			
			
			Iterator<String> attrIter = searchAttrRange.keySet().iterator();
			
			while(attrIter.hasNext())
			{
				String attrName = attrIter.next();
				if( attrsSubspaceInfo.containsKey(attrName) )
				{
					currMaxMatch = currMaxMatch + 1;
					currMatchingAttrList.add(attrName);
				}
			}
			
			
			if(currMaxMatch >= maxMatchingAttrs)
			{
				maxMatchingAttrs = currMaxMatch;
				MaxAttrMatchingStorageClass maxAttrMatchObj = new MaxAttrMatchingStorageClass();
				maxAttrMatchObj.matchingAttrList = currMatchingAttrList;
				maxAttrMatchObj.subspaceId = subspaceId;
				
				if(matchingSubspaceHashMap.containsKey(currMaxMatch))
				{
					matchingSubspaceHashMap.get(currMaxMatch).add(maxAttrMatchObj);
				}
				else
				{
					List<MaxAttrMatchingStorageClass> currMatchingSubspaceNumVector 
													= new LinkedList<MaxAttrMatchingStorageClass>();
					currMatchingSubspaceNumVector.add(maxAttrMatchObj);
					matchingSubspaceHashMap.put(currMaxMatch, currMatchingSubspaceNumVector);
				}
			}
		}
		
		List<MaxAttrMatchingStorageClass> maxMatchingSubspaceNumVector 
			= matchingSubspaceHashMap.get(maxMatchingAttrs);
		
		int returnIndex = new Random().nextInt( maxMatchingSubspaceNumVector.size() );
		//matchingAttributes.clear();

		MaxAttrMatchingStorageClass chosenList = maxMatchingSubspaceNumVector.get(returnIndex);
		
		return subspaceConfigurator.getSubspaceInfoMap().get(chosenList.subspaceId).get(0);
	}
	
	
	protected class MaxAttrMatchingStorageClass
	{
		public int subspaceId;
		public List<String> matchingAttrList;
	}
	
	
	/**
	 * recursive function to generate all the
	 * subspace regions/partitions.
	 */
	public void generateAndStoreSubspaceRegions()
	{
		ContextServiceLogger.getLogger().fine
								(" generateSubspacePartitions() entering " );
		
		Iterator<Integer> subspaceIter = subspaceConfigurator.getSubspaceInfoMap().keySet().iterator();
		
		while( subspaceIter.hasNext() )
		{
			int subspaceId = subspaceIter.next();
			List<SubspaceInfo> replicaVect 
								= subspaceConfigurator.getSubspaceInfoMap().get(subspaceId);
			
			for( int i=0; i<replicaVect.size(); i++ )
			{
				SubspaceInfo subspaceInfo = replicaVect.get(i);
				String tableName = "subspaceId"+subspaceId+"RepNum"+subspaceInfo.getReplicaNum()+"PartitionInfo";
						
				HashMap<String, AttributePartitionInfo> attrsOfSubspace 
										= subspaceInfo.getAttributesOfSubspace();
				
				List<Integer> nodesOfSubspace = subspaceInfo.getNodesOfSubspace();
				
				double numAttr  = attrsOfSubspace.size();
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
				
				for( ICombinatoricsVector<Integer> perm : gen )
				{
					Integer respNodeId = nodesOfSubspace.get(nodeIdCounter%sizeOfNumNodes);
					
					List<Integer> subspacePartitionVector = perm.getVector();
					
					ValueSpaceInfo regionValSpace = convertSubspacePartitionVectorIntoValueSpace
					(subspaceInfo, subspacePartitionVector);
					
					RegionInfo region = new RegionInfo();
					
					region.setValueSpaceInfo(regionValSpace);
					List<Integer> list = new LinkedList<Integer>();
					list.add(respNodeId);
					region.setNodeList(list);

					regionMappingStorage.insertRegionInfoIntoTable(tableName, region);
					nodeIdCounter++;
				}
			}
		}
		ContextServiceLogger.getLogger().fine
							(" generateSubspacePartitions() completed " );
	}
	
	private ValueSpaceInfo convertSubspacePartitionVectorIntoValueSpace
			(SubspaceInfo subspaceInfo, List<Integer> subspacePartitionVector)
	{
		assert(subspaceInfo.getAttributesOfSubspace().size() == subspacePartitionVector.size());
		
		ValueSpaceInfo valSpace = new ValueSpaceInfo();

		HashMap<String, AttributePartitionInfo>  attrSubspaceInfo = subspaceInfo.getAttributesOfSubspace();

		Iterator<String> attrIter = attrSubspaceInfo.keySet().iterator();
		int counter =0;
		while(attrIter.hasNext())
		{
			String attrName = attrIter.next();
			AttributePartitionInfo attrPartInfo = attrSubspaceInfo.get(attrName);
			int partitionNum = subspacePartitionVector.get(counter);
			RangePartitionInfo rangePartInfo 
					= attrPartInfo.getSubspaceDomainPartitionInfo().get(partitionNum);
			// if it is a String then single quotes needs to be added

			valSpace.getValueSpaceBoundary().put(attrName, rangePartInfo.attrValRange);
			counter++;
		}
		return valSpace;
	}
	
	
	public static void main(String[] args) throws PropertyVetoException
	{
		int NUM_ATTRS 			= Integer.parseInt(args[0]);
		int NUM_NODES 			= Integer.parseInt(args[1]);
		int ATTRs_PER_SUBSPACE 	= Integer.parseInt(args[2]);
		
		
		HashMap<String, AttributeMetaInfo> givenMap = new HashMap<String, AttributeMetaInfo>();
		List<String> attrList = new LinkedList<String>();
		
		for(int i=0; i < NUM_ATTRS; i++)
		{
			String attrName = "attr"+i;
			AttributeMetaInfo attrInfo =
					new AttributeMetaInfo(attrName, 1+"", 1500+"", AttributeTypes.DoubleType);
			
			givenMap.put(attrInfo.getAttrName(), attrInfo);	
			attrList.add(attrName);
		}
		
		CSNodeConfig csNodeConfig = new CSNodeConfig();
		for(int i=0; i< NUM_NODES; i++)
		{
			try 
			{
				csNodeConfig.add(i, 
						new InetSocketAddress(InetAddress.getByName("localhost"), 3000+i));
			}
			catch (UnknownHostException e)
			{
				e.printStackTrace();
			}
		}
		
		AttributeTypes.initializeGivenMapAndList(givenMap, attrList);
		HyperdexBasedRegionMappingPolicy obj 
				= new HyperdexBasedRegionMappingPolicy(givenMap, csNodeConfig, 
						ATTRs_PER_SUBSPACE, new SQLiteDataSource(0));
		
		
		obj.computeRegionMapping();
		
		
		String searchQuery = "attr13 >= 321 AND attr13 <= 671 AND  "
				+ "attr2 >= 286 AND attr2 <= 736 AND  attr4 >= 983 AND attr4 <= 1133 AND  "
				+ "attr10 >= 491 AND attr10 <= 641";
		
		HashMap<String, AttributeValueRange> searchAttrValRange  
										= QueryParser.parseQuery(searchQuery);
		
		
		
		ValueSpaceInfo queryValSpace = ValueSpaceInfo.getAllAttrsValueSpaceInfo
						(searchAttrValRange, givenMap);
		
		List<Integer> nodeList = obj.getNodeIDsForAValueSpaceForSearch
															(queryValSpace);
		
		System.out.println("Search node list "+nodeList);
		
		ValueSpaceInfo valSpace = new ValueSpaceInfo();
		
		Random rand = new Random();
		for(int i=0; i<NUM_ATTRS; i++)
		{
			int value = 1+rand.nextInt(1500);
			valSpace.getValueSpaceBoundary().put("attr"+i, 
						new AttributeValueRange(value+"", value+""));
		}
		
		nodeList = obj.getNodeIDsForAValueSpaceForUpdate("", valSpace);
		
		System.out.println("Update node list "+nodeList);
	

		
		// for searches
		BufferedReader br = null;
		FileReader fr = null;
		
		HashMap<Integer, Integer> subspaceQMap = new HashMap<Integer, Integer>();
		try
		{
			fr = new FileReader("traces/guassianTrace/searchFile.txt");
			br = new BufferedReader(fr);
			
			
			while( (searchQuery = br.readLine()) != null )
			{
				searchAttrValRange = QueryParser.parseQuery(searchQuery);
				
				queryValSpace = ValueSpaceInfo.getAllAttrsValueSpaceInfo
											(searchAttrValRange, givenMap);
				
				
				HashMap<String, AttributeValueRange> queryAttrMap 
									 = obj.getNotFullRangeAttrsInSearchQuery(queryValSpace);
				
				SubspaceInfo subInfo = obj.getMaxOverlapSubspace(queryAttrMap );
				
				if(subspaceQMap.containsKey(subInfo.getSubspaceId()))
				{
					int total = subspaceQMap.get(subInfo.getSubspaceId());
					total++;
					subspaceQMap.put(subInfo.getSubspaceId(), total);
				}
				else
				{
					subspaceQMap.put(subInfo.getSubspaceId(), 1);
				}
			}
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				if (br != null)
					br.close();
				
				if (fr != null)
					fr.close();	
			}
			catch (IOException ex)
			{
				ex.printStackTrace();
			}
		}
		
		System.out.println("subspaceQMap "+subspaceQMap.toString());
	}
}
