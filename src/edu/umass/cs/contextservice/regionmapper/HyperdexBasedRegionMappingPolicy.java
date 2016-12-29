package edu.umass.cs.contextservice.regionmapper;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;


import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.configurator.AbstractSubspaceConfigurator;
import edu.umass.cs.contextservice.configurator.BasicSubspaceConfigurator;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.queryparsing.QueryParser;
import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;
import edu.umass.cs.contextservice.regionmapper.helper.RegionInfo;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;



public class HyperdexBasedRegionMappingPolicy extends AbstractRegionMappingPolicy
{
	private final AbstractSubspaceConfigurator subspaceConfigurator;
	
	
	public HyperdexBasedRegionMappingPolicy( HashMap<String, AttributeMetaInfo> attributeMap, 
			CSNodeConfig csNodeConfig, int numberAttrsPerSubspace )
	{
		super(attributeMap, csNodeConfig);
		
		subspaceConfigurator 
			= new BasicSubspaceConfigurator(csNodeConfig, numberAttrsPerSubspace);
	}
	

	@Override
	public void computeRegionMapping() 
	{
		subspaceConfigurator.configureSubspaceInfo();
//		HashMap<Integer, Vector<SubspaceInfo>> subspaceInfoMap 
//					= subspaceConfigurator.getSubspaceInfoMap();
		
		subspaceConfigurator.generateAndStoreSubspaceRegions();	
		
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
			
			
			List<RegionInfo> subsapceRegionsList = subInfo.getSubspaceRegionsList();
			
			
			for(int i=0; i<subsapceRegionsList.size(); i++)
			{
				RegionInfo currRegion = subsapceRegionsList.get(i);
				
				boolean overlap = ValueSpaceInfo.checkOverlapOfTwoValueSpaces
								(attributeMap, currRegion.getValueSpaceInfo(), updateSubspaceValSpace);
				
				if(overlap)
				{
					nodeMap.put(currRegion.getNodeList().get(0), true);
					break;
				}
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
		List<String> queryAttrList = getNotFullRangeAttrsInSearchQuery(valueSpace);
		
		SubspaceInfo subInfo = getMaxOverlapSubspace(queryAttrList );
		
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
		
		//System.out.println("searchSubspaceValSpace "+searchSubspaceValSpace.toString());
		
		
		HashMap<Integer, Boolean> nodeMap = new HashMap<Integer, Boolean>();
		
		List<RegionInfo> subsapceRegionsList = subInfo.getSubspaceRegionsList();
		
		
		for(int i=0; i<subsapceRegionsList.size(); i++)
		{
			RegionInfo currRegion = subsapceRegionsList.get(i);
			
			boolean overlap = ValueSpaceInfo.checkOverlapOfTwoValueSpaces
							(attributeMap, currRegion.getValueSpaceInfo(), searchSubspaceValSpace);
			
			if(overlap)
			{
				nodeMap.put(currRegion.getNodeList().get(0), true);
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
	
	
	private List<String> getNotFullRangeAttrsInSearchQuery(ValueSpaceInfo valueSpace)
	{
		HashMap<String, AttributeValueRange> valSpaceBoundary = valueSpace.getValueSpaceBoundary();
		
		Iterator<String> attrIter = valSpaceBoundary.keySet().iterator();
		List<String> queryAttrList = new LinkedList<String>();
		
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
				queryAttrList.add(attrName);
			}
		}
		return queryAttrList;
	}
	
	
	/**
	 * Returns subspace number of the maximum overlapping
	 * subspace. Used in processing search query.
	 * @return
	 */
	private SubspaceInfo getMaxOverlapSubspace( List<String> queryAttrList)
	{
		// first the maximum matching subspace is found and then any of its replica it chosen
		Iterator<Integer> keyIter   	= subspaceConfigurator.getSubspaceInfoMap().keySet().iterator();
		int maxMatchingAttrs 			= 0;
		
		HashMap<Integer, List<MaxAttrMatchingStorageClass>> matchingSubspaceHashMap = 
				new HashMap<Integer, List<MaxAttrMatchingStorageClass>>();
		
		while( keyIter.hasNext() )
		{
			int subspaceId = keyIter.next();
			SubspaceInfo currSubInfo = subspaceConfigurator.getSubspaceInfoMap().get(subspaceId).get(0);
			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo = currSubInfo.getAttributesOfSubspace();
			
			int currMaxMatch = 0;
			List<String> currMatchingAttrList = new LinkedList<String>();
			
			for(int i=0; i<queryAttrList.size(); i++)
			{
				if( attrsSubspaceInfo.containsKey(queryAttrList.get(i)) )
				{
					currMaxMatch = currMaxMatch + 1;
					currMatchingAttrList.add(queryAttrList.get(i));
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
	
	
	public static void main(String[] args)
	{
		int NUM_ATTRS 			= Integer.parseInt(args[0]);
		int NUM_NODES 			= Integer.parseInt(args[1]);
		int ATTRs_PER_SUBSPACE 	= Integer.parseInt(args[2]);
		
		
		
		HashMap<String, AttributeMetaInfo> givenMap = new HashMap<String, AttributeMetaInfo>();
		
		for(int i=0; i < NUM_ATTRS; i++)
		{
			String attrName = "attr"+i;
			AttributeMetaInfo attrInfo =
					new AttributeMetaInfo(attrName, 1+"", 1500+"", AttributeTypes.DoubleType);
			
			givenMap.put(attrInfo.getAttrName(), attrInfo);	
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
		
		AttributeTypes.initializeGivenMap(givenMap);
		HyperdexBasedRegionMappingPolicy obj 
				= new HyperdexBasedRegionMappingPolicy(givenMap, csNodeConfig, ATTRs_PER_SUBSPACE);
		
		
		obj.computeRegionMapping();
		
		
		String searchQuery = "attr13 >= 321 AND attr13 <= 671 AND  "
				+ "attr2 >= 286 AND attr2 <= 736 AND  attr4 >= 983 AND attr4 <= 1133 AND  "
				+ "attr10 >= 491 AND attr10 <= 641";
		
		ValueSpaceInfo queryValSpace = QueryParser.parseQuery(searchQuery);
		
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
		
	}
}
