package edu.umass.cs.contextservice.regionmapper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.queryparsing.QueryParser;
import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;
import edu.umass.cs.contextservice.regionmapper.helper.RegionInfo;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;
import edu.umass.cs.contextservice.utils.Utils;


public class WorkloadAwareRegionMappingPolicy extends AbstractRegionMappingPolicy
{
	public static final double rho									= 0.5;
	// hyperplane moves with 10% of the total interval.
	private static final double PLANE_MOVING_PERCENTAGE				= 0.1;
	
	private static final String SEARCH_TRACE_FILE					= "traces/guassianTrace/searchFile.txt";
	private static final String UPDATE_TRACE_FILE					= "traces/guassianTrace/updateFile.txt";
	
	// this field is only for testing and will be removed later.
	//private static final double NUM_SEARCH_QUERIES				= 1000.0;
	// we create regions such that 0.98 threshold is achieved.
	//private static final double JAINS_FAIRNESS_THRESHOLD			= 0.90;
	private final LinkedList<RegionInfo> regionList;
	
	
	public WorkloadAwareRegionMappingPolicy(HashMap<String, AttributeMetaInfo> attributeMap, 
			CSNodeConfig nodeConfig)
	{
		super(attributeMap, nodeConfig);
		regionList = new LinkedList<RegionInfo>();
	}
	
	
	@Override
	public List<Integer> getNodeIDsForUpdate(
			String GUID, HashMap<String, AttributeValueRange> attrValRangeMap ) 
	{
		return null;
	}
	
	@Override
	public List<Integer> getNodeIDsForSearch
			(HashMap<String, AttributeValueRange> attrValRangeMap) 
	{
		return null;
	}
	
	
	@Override
	public void computeRegionMapping()
	{
		double numRegions = Math.sqrt(nodeConfig.getNodeIDs().size());
		
		ValueSpaceInfo totalValSpace = new ValueSpaceInfo();
		Vector<String> attrList = new Vector<String>();
		
		// construct the value space.
		Iterator<String> attrIter = attributeMap.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();	
			attrList.add(attrName);
			
			AttributeMetaInfo attrMeta = attributeMap.get(attrName);
			
			AttributeValueRange attrValRange = new AttributeValueRange
									(attrMeta.getMinValue(), attrMeta.getMaxValue());
			
			totalValSpace.getValueSpaceBoundary().put(attrName, attrValRange);
		}
		
		RegionInfo totalValSpaceRegion = new RegionInfo();
		totalValSpaceRegion.setValueSpaceInfo(totalValSpace);
		
		// set region load.
		double searchLoad = computeSearchLoadOnARegionBasedOnTrace( totalValSpaceRegion, 
										attributeMap, nodeConfig.getNodeIDs().size() );	
		
		double updateLoad = computeUpdateLoadOnARegionBasedOnTrace( totalValSpaceRegion, 
				attributeMap, nodeConfig.getNodeIDs().size() );	
		
		
		totalValSpaceRegion.setSearchLoad(searchLoad);
		totalValSpaceRegion.setUpdateLoad(updateLoad);
		
		
		regionList.add(totalValSpaceRegion);
		
		
		// first create numRegions regions.	
		while( regionList.size() < numRegions )
		{
			partitionValueSpaceGreedily(regionList, attributeMap, 
										nodeConfig.getNodeIDs().size() );
		}
		
		// print regions.
		List<Double> sLoadList = new LinkedList<Double>();
		List<Double> uLoadList = new LinkedList<Double>();
		
		for(int i=0; i<regionList.size(); i++)
		{
			RegionInfo currRegion = regionList.get(i);
			
			double volume = computeLogVolume(currRegion.getValueSpaceInfo().getValueSpaceBoundary(), 
														attributeMap );
			
			System.out.println( "Region num "+i+" log volume "+volume+" "
					+" optimalSearchLoad "+currRegion.getSearchLoad()
					+" optimalUpdateLoad "+currRegion.getUpdateLoad()
					+" "+currRegion.toString() );
			
			sLoadList.add(currRegion.getSearchLoad());
			uLoadList.add(currRegion.getUpdateLoad());
		}
		
		double sjfi = Utils.computeJainsFairnessIndex(sLoadList);
		double ujfi = Utils.computeJainsFairnessIndex(uLoadList);
		System.out.println("Search JFI "+sjfi+" update JFI "+ujfi);
		
		writeRegionsToFile(nodeConfig.getNodeIDs().size());
	}
	
	
	private void writeRegionsToFile(int totalNodes)
	{
		BufferedWriter bw 	= null;
		FileWriter fw 		= null;
		
		try
		{
			String fileName = "RegionInfoNumNodes"+totalNodes+".txt";
			fw = new FileWriter(fileName);
			bw = new BufferedWriter(fw);
			
			for( int i=0; i<regionList.size(); i++ )
			{
				RegionInfo regionInf = regionList.get(i);
				bw.write(regionInf.getValueSpaceInfo().toString() +"\n");
			}
		} catch (IOException e) 
		{
			e.printStackTrace();
		}
		finally
		{
			try 
			{
				if (bw != null)
					bw.close();
					
				if (fw != null)
					fw.close();
			} 
			catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
	}
	
	/**
	 * This function partitions a region from a list of regions greedily so that 
	 * the Jains fairness index is maximized.
	 * Returns the input list, whose size is +1 than the size at the function calling time.
	 */
	private void partitionValueSpaceGreedily(LinkedList<RegionInfo> regionList, 
									HashMap<String, AttributeMetaInfo> attributeMap, int totalNodes)
	{
		// index number of the optimal region to split in the regionList
		int optimalIndexNum  = -1;
		
		// optimal hyperplane for the region above.
		HyperplaneInfo optimalHyperplane = null;
		
		double optimalJFI = -1;
		
		for( int i=0; i<regionList.size(); i++ )
		{
			RegionInfo currRegionInfo = regionList.get(i);
			ValueSpaceInfo currRegionVS = currRegionInfo.getValueSpaceInfo();
			
			HashMap<String, AttributeValueRange> currRegionVSBound 
									= currRegionVS.getValueSpaceBoundary();
			
			
			Iterator<String> attrIter = currRegionVSBound.keySet().iterator();
			
			while( attrIter.hasNext() )
			{
				String hyperplaneAttrName = attrIter.next();
				
				
				AttributeMetaInfo attrMetaInfo = attributeMap.get(hyperplaneAttrName);
				
				assert( !attrMetaInfo.getDataType().equals(AttributeTypes.StringType) );
				
				//RegionInfo newRegion = copyValueSpaceToRegion(currRegionVS);
				
				
				AttributeValueRange hyperAttrValRange = currRegionVSBound.get(hyperplaneAttrName);
				
				assert(!attrMetaInfo.getDataType().equals(AttributeTypes.StringType));
				
				
				double lowerBound = Double.parseDouble(hyperAttrValRange.getLowerBound());
				double upperBound = Double.parseDouble(hyperAttrValRange.getUpperBound());
				
				double range = upperBound - lowerBound;
				
				
				double currPlane = lowerBound + PLANE_MOVING_PERCENTAGE*range;
				
				while( currPlane < upperBound )
				{
					AttributeValueRange attrValRange1 
								= new AttributeValueRange(lowerBound+"", currPlane+"");
					
					AttributeValueRange attrValRange2 
								= new AttributeValueRange(currPlane+"", upperBound+"");
					
					
					RegionInfo splitRegion1 = copyValueSpaceToRegion(currRegionVS);
					RegionInfo splitRegion2 = copyValueSpaceToRegion(currRegionVS);
					
					
					splitRegion1.getValueSpaceInfo().getValueSpaceBoundary().put
																(hyperplaneAttrName, attrValRange1);
					
					splitRegion2.getValueSpaceInfo().getValueSpaceBoundary().put
																(hyperplaneAttrName, attrValRange2);
					
					
					
//					double currLoad1 = computeLoadOnARegionBasedOnTrace( splitRegion1, 
//																attributeMap, totalNodes );
//					splitRegion1.setTraceLoad(currLoad1);
					
					double searchLoad1 = computeSearchLoadOnARegionBasedOnTrace( splitRegion1, 
							attributeMap, nodeConfig.getNodeIDs().size() );	

					double updateLoad1 = computeUpdateLoadOnARegionBasedOnTrace( splitRegion1, 
							attributeMap, nodeConfig.getNodeIDs().size() );	


					splitRegion1.setSearchLoad(searchLoad1);
					splitRegion1.setUpdateLoad(updateLoad1);

					
					
//					double currLoad2 = computeLoadOnARegionBasedOnTrace( splitRegion2, 
//													attributeMap, totalNodes );
//					splitRegion2.setTraceLoad(currLoad2);
				
					
					double searchLoad2 = computeSearchLoadOnARegionBasedOnTrace( splitRegion2, 
							attributeMap, nodeConfig.getNodeIDs().size() );	

					double updateLoad2 = computeUpdateLoadOnARegionBasedOnTrace( splitRegion2, 
							attributeMap, nodeConfig.getNodeIDs().size() );	


					splitRegion2.setSearchLoad(searchLoad2);
					splitRegion2.setUpdateLoad(updateLoad2);
					
					
					List<Double> sLoadList = new LinkedList<Double>(); 
					List<Double> uLoadList = new LinkedList<Double>(); 
					for(int j=0; j<regionList.size(); j++)
					{
						// not taking the load of region we are splitting now,
						if(i != j)
						{
							sLoadList.add(regionList.get(j).getSearchLoad());
							uLoadList.add(regionList.get(j).getUpdateLoad());
						}
					}
					sLoadList.add(searchLoad1);
					sLoadList.add(searchLoad2);
					
					uLoadList.add(updateLoad1);
					uLoadList.add(updateLoad2);
					
					double sjfi = Utils.computeJainsFairnessIndex(sLoadList);
					double ujfi = Utils.computeJainsFairnessIndex(uLoadList);
					
					double jfi = rho * sjfi + (1-rho) * ujfi;
					
					if( optimalJFI == -1 )
					{
						optimalIndexNum = i;
						optimalHyperplane = new HyperplaneInfo(hyperplaneAttrName, 
								splitRegion1.getValueSpaceInfo().getValueSpaceBoundary().get
										(hyperplaneAttrName).getUpperBound());
						
						optimalJFI =jfi;
					}
					else
					{	
						if(jfi > optimalJFI)
						{
							optimalIndexNum = i;
							optimalHyperplane = new HyperplaneInfo(hyperplaneAttrName, 
									splitRegion1.getValueSpaceInfo().getValueSpaceBoundary().get
											(hyperplaneAttrName).getUpperBound());
							
							optimalJFI =jfi;
						}
					}
					
					currPlane = currPlane + PLANE_MOVING_PERCENTAGE*range;
				}
				
			}
		}
		
		// we should have greedily optimal hyperplane and region to split by now.
		
		RegionInfo splitRegion = regionList.remove(optimalIndexNum);
		
		RegionInfo regionOne = copyValueSpaceToRegion(splitRegion.getValueSpaceInfo());
		
		AttributeValueRange originalAttrVal 
			= splitRegion.getValueSpaceInfo().getValueSpaceBoundary().get(optimalHyperplane.hyperplaneAttrName);

		
		AttributeValueRange regionOneAttrVal = new AttributeValueRange(
				originalAttrVal.getLowerBound(), optimalHyperplane.hyperplaneVal);

		regionOne.getValueSpaceInfo().getValueSpaceBoundary().put
			(optimalHyperplane.hyperplaneAttrName, regionOneAttrVal);

		double sLoad = computeSearchLoadOnARegionBasedOnTrace(regionOne, attributeMap, 
									totalNodes );
		
		double uLoad = computeUpdateLoadOnARegionBasedOnTrace(regionOne, attributeMap, 
				totalNodes );

		regionOne.setSearchLoad(sLoad);
		regionOne.setUpdateLoad(uLoad);


		RegionInfo regionTwo = copyValueSpaceToRegion(splitRegion.getValueSpaceInfo());

		/// newRegionAttrVal.getUpperBound() is the hyperplane val.
		AttributeValueRange regionTwoAttrVal = new AttributeValueRange(
				optimalHyperplane.hyperplaneVal, originalAttrVal.getUpperBound());
		
		regionTwo.getValueSpaceInfo().getValueSpaceBoundary().put
			(optimalHyperplane.hyperplaneAttrName, regionTwoAttrVal);
		
		
		sLoad = computeSearchLoadOnARegionBasedOnTrace(regionTwo, attributeMap, 
				totalNodes );
		
		uLoad = computeUpdateLoadOnARegionBasedOnTrace(regionTwo, attributeMap, 
				totalNodes );

		regionTwo.setSearchLoad(sLoad);
		regionTwo.setUpdateLoad(uLoad);
		
		
		regionList.add(regionOne);
		regionList.add(regionTwo);
		
		System.out.println("Optimal index "+optimalIndexNum+" val space " 
						+ " Optimal attr "+optimalHyperplane.hyperplaneAttrName
						+ " Optimal val "+optimalHyperplane.hyperplaneVal
						+ " Optimal JFI "+ optimalJFI
						+ splitRegion.getValueSpaceInfo().toString());
	}
	
	
	private RegionInfo copyValueSpaceToRegion(ValueSpaceInfo valueSpace)
	{
		ValueSpaceInfo regionValueSpace = new ValueSpaceInfo();
		
		Iterator<String> attrIter = valueSpace.getValueSpaceBoundary().keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			AttributeValueRange valspaceAttrValRange = valueSpace.getValueSpaceBoundary().get(attrName);
			
			AttributeValueRange regionAttrValRange = new AttributeValueRange
								( valspaceAttrValRange.getLowerBound(), 
										valspaceAttrValRange.getUpperBound() );
			
			regionValueSpace.getValueSpaceBoundary().put(attrName, regionAttrValRange);
		}
		
		RegionInfo regionInfo = new RegionInfo();
		regionInfo.setValueSpaceInfo(regionValueSpace);
		
		return regionInfo;
	}
	
	
	/**
	 * Computes the volume of region in log scale.
	 * Volume of high dimensional space could be large so taking log
	 * @return
	 */
	private double computeLogVolume(HashMap<String, AttributeValueRange> valueSpaceBoundary, 
			HashMap<String, AttributeMetaInfo> attributeMap)
	{
		Iterator<String> attrIter = valueSpaceBoundary.keySet().iterator();
		
		double logSum = 0;
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			
			AttributeMetaInfo attrMetaInfo = attributeMap.get(attrName);
			
			AttributeValueRange attrValRange = valueSpaceBoundary.get(attrName);		
			
			double intervalSize = attrMetaInfo.computeRangeSize(attrValRange.getLowerBound(), 
												attrValRange.getUpperBound());
			
			logSum = logSum + Math.log(intervalSize);
		}
		return logSum;
	}
	
	
	private double computeSearchLoadOnARegionBasedOnTrace( RegionInfo regionInfo, 
			HashMap<String, AttributeMetaInfo> attributeMap, int totalNodes )
	{	
		// for searches
		BufferedReader br = null;
		FileReader fr = null;
		
		double overlapSearchQueries = 0.0;
		
		double searchQueryProb = 0.0;
		double totalSearchQueries = 0.0;
		
		try
		{
			fr = new FileReader(SEARCH_TRACE_FILE);
			br = new BufferedReader(fr);
			
			String searchQuery;
			
			
			while( (searchQuery = br.readLine()) != null )
			{
				if( checkIfQueryAndRegionOverlap(searchQuery, regionInfo, attributeMap) )
				{
					overlapSearchQueries++;
				}
				totalSearchQueries++;
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
		searchQueryProb = overlapSearchQueries/totalSearchQueries;
		
		return searchQueryProb;
	}
	
	
	private double computeUpdateLoadOnARegionBasedOnTrace( RegionInfo regionInfo, 
			HashMap<String, AttributeMetaInfo> attributeMap, int totalNodes )
	{
		BufferedReader br = null;
		FileReader fr = null;
		
		// calculating update probability
		double overlapUpdateRequests = 0.0;
		
		double updateRequestProb = 0.0;
		double totalUpdateRequests = 0.0;
		
		try
		{
			fr = new FileReader(UPDATE_TRACE_FILE);
			br = new BufferedReader(fr);
			
			String updateRequest;
			
			while( (updateRequest = br.readLine()) != null )
			{
				if( checkIfUpdateRequestRegionOverlap(updateRequest, regionInfo, attributeMap) )
				{
					overlapUpdateRequests++;
				}
				totalUpdateRequests++;
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
		
		updateRequestProb = overlapUpdateRequests/totalUpdateRequests;
		
		return updateRequestProb;
	}
	
	
	private boolean checkIfQueryAndRegionOverlap(String searchQuery, 
			RegionInfo regionInfo, HashMap<String, AttributeMetaInfo> attributeMap )
	{
		HashMap<String, AttributeValueRange> queryAttrValMap = QueryParser.parseQuery(searchQuery);
		
		ValueSpaceInfo queryValspace = 
					ValueSpaceInfo.getAllAttrsValueSpaceInfo(queryAttrValMap, attributeMap);
		

		return ValueSpaceInfo.checkOverlapOfTwoValueSpaces(attributeMap, 
					queryValspace, regionInfo.getValueSpaceInfo());
	}
	
	
	private boolean checkIfUpdateRequestRegionOverlap(String updateRequest, 
			RegionInfo regionInfo, HashMap<String, AttributeMetaInfo> attributeMap )
	{
		// format of updateRequest is first term is GUID, then update attrName, update AttrValue and 
		// then rest of the attribute value pairs.
		String[] parsed = updateRequest.split(",");
		
		//String guid = parsed[0];
		
		String uAttrName = parsed[1];
		String uAttrVal = parsed[2];
		
		// get JSON with old values.
		ValueSpaceInfo oldValSpace = new ValueSpaceInfo();
		
		int currPos = 3;
		while(currPos < parsed.length)
		{
			String currAttr = parsed[currPos];
			currPos++;
			String currVal = parsed[currPos];
			currPos++;
			
			oldValSpace.getValueSpaceBoundary().put(currAttr, 
						new AttributeValueRange(currVal, currVal));
		}
		
		// first check if old value overlaps with this region for deletion of GUID
		
		boolean oldValSpaceOverlap = ValueSpaceInfo.checkOverlapOfTwoValueSpaces
						(attributeMap, regionInfo.getValueSpaceInfo(), oldValSpace);
		
		// make old json to new json 
		oldValSpace.getValueSpaceBoundary().put
				(uAttrName, new AttributeValueRange(uAttrVal, uAttrVal));
		
		
		boolean newValSpaceOverlap = ValueSpaceInfo.checkOverlapOfTwoValueSpaces
				(attributeMap, regionInfo.getValueSpaceInfo(), oldValSpace);
		
		return oldValSpaceOverlap || newValSpaceOverlap;
	}
	
	
	
	
	private class HyperplaneInfo
	{
		private final String hyperplaneAttrName;
		private final String hyperplaneVal;
		
		public HyperplaneInfo(String hyperplaneAttrName, String hyperplaneVal)
		{
			this.hyperplaneAttrName = hyperplaneAttrName;
			this.hyperplaneVal = hyperplaneVal;
		}
		
//		public String getHyperplaneAttrName()
//		{
//			return hyperplaneAttrName;
//		}
//		
//		public String getHyperplaneAttrVal()
//		{
//			return hyperplaneVal;
//		}
	}
	
	
	public static void main(String[] args)
	{
		
		int NUM_ATTRS = 20;
		int[] nodeList = {1, 4, 9, 16, 25, 36, 49, 64, 81, 100, 121};
		//int[] nodeList = {121};
		
		for(int n=0; n<nodeList.length; n++)
		{
			int NUM_NODES = nodeList[n];
			
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
			
			WorkloadAwareRegionMappingPolicy obj 
					= new WorkloadAwareRegionMappingPolicy(givenMap, csNodeConfig);
			
			obj.computeRegionMapping();
		}
	}
	
}


/**
 * Partitions the given ValueSpace with the given attribute
 * and returns the newly created Region. But the original value 
 * space is not changed. The load on the region is also 
 * returned in the RegionInfo object returned.
 * 
 * @return
 */
/*private RegionInfo partitionValueSpaceWithAttr(ValueSpaceInfo valueSpace, 
		HashMap<String, AttributeMetaInfo> attributeMap, 
		int totalNodes, String hyperplaneAttrName)
{
	double optimalHyperplane = -1;
	double optimalLoad = -1;
	
	AttributeMetaInfo attrMetaInfo = attributeMap.get(hyperplaneAttrName);
	
	assert( !attrMetaInfo.getDataType().equals(AttributeTypes.StringType) );
	
	HashMap<String, AttributeValueRange> valSpaceBoundary = valueSpace.getValueSpaceBoundary();
	
	ValueSpaceInfo regionValueSpace = new ValueSpaceInfo();
	
	Iterator<String> attrIter = valSpaceBoundary.keySet().iterator();
	
	while( attrIter.hasNext() )
	{
		String attrName = attrIter.next();
		AttributeValueRange valspaceAttrValRange = valSpaceBoundary.get(attrName);
		
		AttributeValueRange regionAttrValRange = new AttributeValueRange
							( valspaceAttrValRange.getLowerBound(), 
									valspaceAttrValRange.getUpperBound() );
		
		regionValueSpace.getValueSpaceBoundary().put(attrName, regionAttrValRange);
	}
	
	RegionInfo regionInfo = new RegionInfo();
	regionInfo.setValueSpaceInfo(regionValueSpace);
	
	
	AttributeValueRange hyperAttrValRange = valSpaceBoundary.get(hyperplaneAttrName);
	
	assert(!attrMetaInfo.getDataType().equals(AttributeTypes.StringType));
	
	
	double lowerBound = Double.parseDouble(hyperAttrValRange.getLowerBound());
	double upperBound = Double.parseDouble(hyperAttrValRange.getUpperBound());
	
	double range = upperBound - lowerBound;
	
	
	double currPlane = lowerBound + PLANE_MOVING_PERCENTAGE*range;
	
	while( currPlane < upperBound )
	{
		AttributeValueRange attrValRange 
					= new AttributeValueRange(lowerBound+"", currPlane+"");
		
		regionValueSpace.getValueSpaceBoundary().put(hyperplaneAttrName, attrValRange);
		
		
		double currLoad = computeLoadOnARegionBasedOnTrace( regionInfo, 
													attributeMap, totalNodes );
		
		if( optimalLoad == -1 )
		{
			optimalLoad = currLoad;
			optimalHyperplane = currPlane;
		}
		else
		{
			// see the explanation for 1.0 in the heuristic scheme draft.
			// the load that is returned should be close to 1.0
			// close to 1.0 means that a single node is near to its capacity.
			// it is not lightly loaded < 1 case and more loaded > 1 case.
			if( Math.abs(currLoad-1.0) < Math.abs(optimalLoad-1.0) )
			{
				optimalLoad = currLoad;
				optimalHyperplane = currPlane;
			}
		}
		currPlane = currPlane + PLANE_MOVING_PERCENTAGE*range;
	}
	
	// setting region with optimal hyperplane
	AttributeValueRange regionAttrValRange = new AttributeValueRange(
			hyperAttrValRange.getLowerBound(), optimalHyperplane+"");
	regionValueSpace.getValueSpaceBoundary().put(hyperplaneAttrName, regionAttrValRange);
	regionInfo.setTraceLoad(optimalLoad);
	
	return regionInfo;
}*/