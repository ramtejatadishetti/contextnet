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

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;
import edu.umass.cs.contextservice.regionmapper.helper.RegionInfo;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;
import edu.umass.cs.contextservice.utils.Utils;


public class WorkloadAwareRegionMappingPolicy extends AbstractRegionMappingPolicy
{
	// hyperplane moves with 10% of the total interval.
	private static final double PLANE_MOVING_PERCENTAGE			= 0.1;
	
	private static final String SEARCH_TRACE_FILE				= "traces/guassianTrace/searchFile.txt";
	private static final String UPDATE_TRACE_FILE				= "traces/guassianTrace/updateFile.txt";
	
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
	public List<Integer> getNodeIDsForAValueSpace(
			ValueSpaceInfo valueSpace, REQUEST_TYPE requestType ) 
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
		double regionLoad = computeLoadOnARegionBasedOnTrace( totalValSpaceRegion, 
										attributeMap, nodeConfig.getNodeIDs().size() );
		
		
		totalValSpaceRegion.setTraceLoad(regionLoad);
		
		regionList.add(totalValSpaceRegion);
		
		
		// first create numRegions regions.	
		while( regionList.size() < numRegions )
		{
			partitionValueSpaceGreedily(regionList, attributeMap, 
										nodeConfig.getNodeIDs().size() );
		}
		
		// print regions.
		List<Double> loadList = new LinkedList<Double>();
		
		for(int i=0; i<regionList.size(); i++)
		{
			RegionInfo currRegion = regionList.get(i);
			
			double volume = computeLogVolume(currRegion.getValueSpaceInfo().getValueSpaceBoundary(), 
														attributeMap );
			
			System.out.println( "Region num "+i+" log volume "+volume+" "+" optimalLoad "
					+currRegion.getTraceLoad()+" "+currRegion.toString() );
			
			loadList.add(currRegion.getTraceLoad());
		}
		
		double jfi = Utils.computeJainsFairnessIndex(loadList);
		System.out.println("JFI on load "+jfi);
		
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
				
				RegionInfo newRegion = partitionValueSpaceWithAttr(currRegionVS, 
							attributeMap,  totalNodes, hyperplaneAttrName);
				
				
				// check JFI for all regions including the old and new regions and store if the current
				// one is giving more JFI.
				List<Double> loadList = new LinkedList<Double>(); 
				for(int j=0; j<regionList.size(); j++)
				{
					// not taking the load of region we are splitting now,
					if(i != j)
					{
						loadList.add(regionList.get(j).getTraceLoad());
					}
				}
				
				// now we take the load of the two sub regions of the region we are splitting.
				loadList.add(newRegion.getTraceLoad());
				
				RegionInfo remRegionInfo = copyValueSpaceToRegion(currRegionVS);
				
				AttributeValueRange originalAttrVal 
						= currRegionVS.getValueSpaceBoundary().get(hyperplaneAttrName);
				
				AttributeValueRange newRegionAttrVal = newRegion.getValueSpaceInfo().getValueSpaceBoundary().get(hyperplaneAttrName);
				
				/// newRegionAttrVal.getUpperBound() is the hyperplane val.
				AttributeValueRange remRegionAttrVal = new AttributeValueRange(
							newRegionAttrVal.getUpperBound(), originalAttrVal.getUpperBound());
				
				remRegionInfo.getValueSpaceInfo().getValueSpaceBoundary().put
							(hyperplaneAttrName, remRegionAttrVal);
				
				double regionLoad = computeLoadOnARegionBasedOnTrace(remRegionInfo, attributeMap, 
						totalNodes );

				remRegionInfo.setTraceLoad(regionLoad);
				
				loadList.add(remRegionInfo.getTraceLoad());
				
				
				double jfi = Utils.computeJainsFairnessIndex(loadList);
				
				
				if( optimalJFI == -1 )
				{
					optimalIndexNum = i;
					optimalHyperplane = new HyperplaneInfo(hyperplaneAttrName, 
							newRegion.getValueSpaceInfo().getValueSpaceBoundary().get(hyperplaneAttrName).getUpperBound());
					
					optimalJFI =jfi;
				}
				else
				{	
					if(jfi > optimalJFI)
					{
						optimalIndexNum = i;
						optimalHyperplane = new HyperplaneInfo(hyperplaneAttrName, 
								newRegion.getValueSpaceInfo().getValueSpaceBoundary().get(hyperplaneAttrName).getUpperBound());
						
						optimalJFI =jfi;
					}
				}
				
			}
		}
		
		RegionInfo splitRegion = regionList.remove(optimalIndexNum);
		
		RegionInfo regionOne = copyValueSpaceToRegion(splitRegion.getValueSpaceInfo());
		
		AttributeValueRange originalAttrVal 
			= splitRegion.getValueSpaceInfo().getValueSpaceBoundary().get(optimalHyperplane.hyperplaneAttrName);

		
		AttributeValueRange regionOneAttrVal = new AttributeValueRange(
				originalAttrVal.getLowerBound(), optimalHyperplane.hyperplaneVal);

		regionOne.getValueSpaceInfo().getValueSpaceBoundary().put
			(optimalHyperplane.hyperplaneAttrName, regionOneAttrVal);

		double regionLoad = computeLoadOnARegionBasedOnTrace(regionOne, attributeMap, 
									totalNodes );

		regionOne.setTraceLoad(regionLoad);


		RegionInfo regionTwo = copyValueSpaceToRegion(splitRegion.getValueSpaceInfo());

		/// newRegionAttrVal.getUpperBound() is the hyperplane val.
		AttributeValueRange regionTwoAttrVal = new AttributeValueRange(
				optimalHyperplane.hyperplaneVal, originalAttrVal.getUpperBound());
		
		regionTwo.getValueSpaceInfo().getValueSpaceBoundary().put
			(optimalHyperplane.hyperplaneAttrName, regionTwoAttrVal);
		
		regionLoad = computeLoadOnARegionBasedOnTrace(regionTwo, attributeMap, 
				totalNodes );

		regionTwo.setTraceLoad(regionLoad);		
		
		
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
	
	/**
	 * Partitions the given ValueSpace with the given attribute
	 * and returns the newly created Region. But the original value 
	 * space is not changed. The load on the region is also 
	 * returned in the RegionInfo object returned.
	 * 
	 * @return
	 */
	private RegionInfo partitionValueSpaceWithAttr(ValueSpaceInfo valueSpace, 
			HashMap<String, AttributeMetaInfo> attributeMap, int totalNodes, String hyperplaneAttrName)
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
	}
	
	
	private double computeLoadOnARegionBasedOnTrace( RegionInfo regionInfo, 
			HashMap<String, AttributeMetaInfo> attributeMap, int totalNodes )
	{
		double loadOnRegion = 0.0;
		
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
		
		// search/search+update ration.
		double rho = totalSearchQueries/(totalSearchQueries + totalUpdateRequests);
		
		
		loadOnRegion = rho*searchQueryProb + (1-rho) * Math.sqrt(totalNodes)* updateRequestProb;
		
		return loadOnRegion;
	}
	
	
	private boolean checkIfQueryAndRegionOverlap(String searchQuery, 
			RegionInfo regionInfo, HashMap<String, AttributeMetaInfo> attributeMap )
	{
		QueryInfo qInfo = new QueryInfo(searchQuery);
		
		HashMap<String, ProcessingQueryComponent> qCompMap = qInfo.getProcessingQC();
		
		HashMap<String, AttributeValueRange> regionBoundary 
							= regionInfo.getValueSpaceInfo().getValueSpaceBoundary();
		
		Iterator<String> attrIter = qCompMap.keySet().iterator();
		
		boolean overlap = true;
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			
			ProcessingQueryComponent pqc = qCompMap.get(attrName);
			
			AttributeValueRange regionAttrRange = regionBoundary.get(attrName);
			
			AttributeValueRange queryAttrRange 
					= new AttributeValueRange( pqc.getLowerBound(), pqc.getUpperBound() );
			
			overlap = overlap && AttributeTypes.checkOverlapOfTwoIntervals
					( regionAttrRange, queryAttrRange, attributeMap.get(attrName).getDataType() );
			
			if(!overlap)
				break;		
		}
		return overlap;
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
		JSONObject oldValJson = new JSONObject();
		
		int currPos = 3;
		while(currPos < parsed.length)
		{
			String currAttr = parsed[currPos];
			currPos++;
			String currVal = parsed[currPos];
			currPos++;
			
			try 
			{
				oldValJson.put(currAttr, currVal);
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
		
		// first check if old value overlaps with this region for deletion of GUID
		
		boolean oldValJsonOverlap = checkAttrValJSONRegionOverlap( regionInfo, 
															oldValJson, attributeMap );
		
		// make old json to new json 
		try
		{
			oldValJson.put(uAttrName, uAttrVal);
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		
		boolean newValJsonOverlap = checkAttrValJSONRegionOverlap( regionInfo, 
				oldValJson, attributeMap );
		
		return oldValJsonOverlap || newValJsonOverlap;
	}
	
	
	private boolean checkAttrValJSONRegionOverlap( RegionInfo regionInfo, JSONObject attrValJSON, 
			HashMap<String, AttributeMetaInfo> attributeMap )
	{
		boolean overlap = true;
		
		HashMap<String, AttributeValueRange>  valSpaceBoundary = 
				regionInfo.getValueSpaceInfo().getValueSpaceBoundary();
		
		// JSON iterator warning suppressed
		@SuppressWarnings("unchecked")
		Iterator<String> attrIter = attrValJSON.keys();
		while(attrIter.hasNext())
		{
			String attrName = attrIter.next();
			AttributeMetaInfo attrMetaInfo = attributeMap.get(attrName);
			
			try
			{
				String attrVal = attrValJSON.getString(attrName);
				AttributeValueRange interval1 = new AttributeValueRange(attrVal, attrVal);
				AttributeValueRange interval2 = valSpaceBoundary.get(attrName);	
				
				overlap = overlap && AttributeTypes.checkOverlapOfTwoIntervals(interval1, interval2, 
																			attrMetaInfo.getDataType());
				
				if(!overlap)
				{
					break;
				}	
			} 
			catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
		
		return overlap;	
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
		int NUM_ATTRS = Integer.parseInt(args[0]);
		int NUM_NODES = Integer.parseInt(args[1]);
		
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
		WorkloadAwareRegionMappingPolicy obj 
				= new WorkloadAwareRegionMappingPolicy(givenMap, csNodeConfig);
		
		obj.computeRegionMapping();
	}
}