package edu.umass.cs.contextservice.regionmapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Vector;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;
import edu.umass.cs.contextservice.regionmapper.helper.RegionInfo;
import edu.umass.cs.contextservice.regionmapper.helper.RegionLoadComparator;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;
import edu.umass.cs.contextservice.utils.Utils;


public class WorkloadAwareNormIntervalRegionMappingPolicy extends AbstractRegionMappingPolicy
{
	// hyperplane moves with 10% of the total interval.
	private static final double PLANE_MOVING_PERCENTAGE			= 0.1;
	
	private static final String SEARCH_TRACE_FILE				= "traces/guassianTrace/searchFile.txt";
	private static final String UPDATE_TRACE_FILE				= "traces/guassianTrace/updateFile.txt";
	
	// this field is only for testing and will be removed later.
	//private static final double NUM_SEARCH_QUERIES				= 1000.0;
	// we create regions such that 0.98 threshold is achieved.
	//private static final double JAINS_FAIRNESS_THRESHOLD		= 0.90;
	
	
	private final PriorityQueue<RegionInfo> priorityQueue;
	
	public WorkloadAwareNormIntervalRegionMappingPolicy( 
			HashMap<String, AttributeMetaInfo> attributeMap, 
			List<Integer> nodeIDList )
	{
		super(attributeMap, nodeIDList);
		priorityQueue = new PriorityQueue<RegionInfo>(10, new RegionLoadComparator());
	}
	
	@Override
	public List<Integer> getNodeIDsForAValueSpace(HashMap<String, AttributeValueRange> valueSpaceDef) 
	{
		return null;
	}
	
	@Override
	public void computeRegionMapping()
	{
		double numRegions = Math.sqrt(nodeIDList.size());
		
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
													attributeMap, nodeIDList.size() );
		
		totalValSpaceRegion.setTraceLoad(regionLoad);
		
		priorityQueue.offer(totalValSpaceRegion);
		
		
		// first create numRegions regions.	
		while( priorityQueue.size() < numRegions )
		{	
			RegionInfo poppedRegion = priorityQueue.poll();
			
			System.out.println("Popped region load "+poppedRegion.getTraceLoad()
						+poppedRegion.getValueSpaceInfo().toString());
			
			assert(poppedRegion != null);
			
			String hyperplaneAttr = getLargestNormalizedRangeAttribute
					(poppedRegion, attributeMap);
			
			ValueSpaceInfo currValSpace = poppedRegion.getValueSpaceInfo(); 
			
			RegionInfo newRegionInfo = partitionValueSpaceUsingHyperplane
					(currValSpace, hyperplaneAttr, attributeMap, nodeIDList.size() );
			
			
			priorityQueue.offer(newRegionInfo);
			
			// create a region for the remaining value space and add it in the queue.s
			RegionInfo remRegionInfo = new RegionInfo();	
			remRegionInfo.setValueSpaceInfo(currValSpace);
			
			// compute trace load and set it in the region.
			regionLoad = computeLoadOnARegionBasedOnTrace(remRegionInfo, attributeMap, 
																nodeIDList.size() );
			
			remRegionInfo.setTraceLoad(regionLoad);
			
			priorityQueue.offer(remRegionInfo);
		}
		
		Iterator<RegionInfo> regionIter = priorityQueue.iterator();
		
		// print regions.
		List<Double> loadList = new LinkedList<Double>();
		
		
		int regionNum = 0;
		while( regionIter.hasNext() )
		{
			RegionInfo currRegion = regionIter.next();
			
			double volume = computeLogVolume(currRegion.getValueSpaceInfo().getValueSpaceBoundary(), 
														attributeMap );
			
			System.out.println( "Region num "+regionNum+" log volume "+volume+" "+" optimalLoad "
					+currRegion.getTraceLoad()+" "+currRegion.toString() );
			
			loadList.add(currRegion.getTraceLoad());
		}
		
		double jfi = Utils.computeJainsFairnessIndex(loadList);
		System.out.println("JFI on load "+jfi);
	}
	
	
	private String getLargestNormalizedRangeAttribute(RegionInfo regionInfo, 
								HashMap<String, AttributeMetaInfo> attributeMap)
	{
		String optimalAttrName = "";
		double largestRatio = -1;
		
		HashMap<String, AttributeValueRange> regionBoundaryMap 
											= regionInfo.getValueSpaceInfo().getValueSpaceBoundary();
		
		Iterator<String> attrIter = regionBoundaryMap.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			
			AttributeValueRange attrValRange = regionBoundaryMap.get(attrName);
			
			
			AttributeMetaInfo attrMetaInfo = attributeMap.get(attrName);
			
			double intervalRatio = attrMetaInfo.computeIntervalToRangeRatio(attrValRange);
			
			if( intervalRatio > largestRatio )
			{
				largestRatio = intervalRatio;
				optimalAttrName = attrName;
			}
		}
		
		assert(attributeMap.containsKey(optimalAttrName));
		return optimalAttrName;
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
	 * This function creates a region based on the workload trace.
	 * Right now this function assumes only Int, Long, Double data type.
	 * @param valueSpace
	 * @param hyperplaneAttrName
	 * @param desiredVolumeInLog
	 * @param attributeMap
	 * @return
	 */
	private  RegionInfo partitionValueSpaceUsingHyperplane
		( ValueSpaceInfo valueSpace, String hyperplaneAttrName, 
				HashMap<String, AttributeMetaInfo> attributeMap, int totalNodes )
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
		
		
		AttributeValueRange valSpaceAttrValRange = new AttributeValueRange
				(optimalHyperplane+"", hyperAttrValRange.getUpperBound());
		
		valSpaceBoundary.put(hyperplaneAttrName, valSpaceAttrValRange);
		
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
	
	
	public static void main(String[] args)
	{
		int NUM_ATTRS = 20;
		int NUM_NODES = 100;
		
		HashMap<String, AttributeMetaInfo> givenMap 
						= new HashMap<String, AttributeMetaInfo>();
		
		for(int i=0; i < NUM_ATTRS; i++)
		{
			String attrName = "attr"+i;
			AttributeMetaInfo attrInfo =
					new AttributeMetaInfo(attrName, 1+"", 1500+"", AttributeTypes.DoubleType);
			
			givenMap.put(attrInfo.getAttrName(), attrInfo);	
		}
		
		List<Integer> nodeIDList = new LinkedList<Integer>();
		
		for(int i=0; i< NUM_NODES; i++)
		{
			nodeIDList.add(i);
		}
		
		AttributeTypes.initializeGivenMap(givenMap);
		WorkloadAwareNormIntervalRegionMappingPolicy obj 
				= new WorkloadAwareNormIntervalRegionMappingPolicy(givenMap, nodeIDList);
		
		obj.computeRegionMapping();
	}
}