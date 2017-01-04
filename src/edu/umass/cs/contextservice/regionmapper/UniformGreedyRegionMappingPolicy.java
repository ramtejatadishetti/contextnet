package edu.umass.cs.contextservice.regionmapper;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;
import edu.umass.cs.contextservice.regionmapper.helper.RegionInfo;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;


/**
 * This class implements a uniform region mapping policy. 
 * 
 * This policy creates regions by keeping the volume of regions 
 * approximately same across the regions. 
 * @author ayadav
 */
public class UniformGreedyRegionMappingPolicy extends AbstractRegionMappingPolicy
{
	private final List<RegionInfo> regionList;
	private final Random randGen;
	
	public UniformGreedyRegionMappingPolicy( HashMap<String, AttributeMetaInfo> attributeMap, 
			CSNodeConfig nodeConfig )
	{
		super(attributeMap, nodeConfig);
		regionList = new LinkedList<RegionInfo>();
		randGen = new Random();
	}
	
	@Override
	public List<Integer> getNodeIDsForUpdate
					(String GUID, HashMap<String, AttributeValueRange> attrValRangeMap) 
	{
		// map so that we remove duplicates.
		//FIXME: this code is copied in many policies need to find a way to not copy code.
		HashMap<Integer, Integer> overlapNodeIdsMap = new HashMap<Integer, Integer>();
		
		for( int i=0; i<regionList.size(); i++ )
		{
			RegionInfo currRegion = regionList.get(i);
			ValueSpaceInfo regionValSpace = currRegion.getValueSpaceInfo();
			
			boolean overlap = true;
			
			Iterator<String> inputAttrIter = attrValRangeMap.keySet().iterator();
			
			while( inputAttrIter.hasNext() )
			{
				String attrName = inputAttrIter.next();
				
				AttributeMetaInfo attrMetaInfo = attributeMap.get(attrName);
				
				AttributeValueRange inputAttrValRange  
									= attrValRangeMap.get(attrName);
				
				AttributeValueRange regionAttrValRange 
								= regionValSpace.getValueSpaceBoundary().get(attrName);
				
				
				overlap = overlap && AttributeTypes.checkOverlapOfTwoIntervals(inputAttrValRange, 
												regionAttrValRange, attrMetaInfo.getDataType());
				
				if(!overlap)
				{
					break;
				}
			}
			
			if( overlap )
			{
				// Current region's value space overlaps with the value space in 
				// the input
				List<Integer> regionNodeList = currRegion.getNodeList();
					
				for( int j=0; j<regionNodeList.size(); j++ )
				{
					overlapNodeIdsMap.put(regionNodeList.get(j), regionNodeList.get(j) );
				}
			}
		}
		
		List<Integer> overlapNodeIds = new LinkedList<Integer>();
		Iterator<Integer> nodeIdIter = overlapNodeIdsMap.keySet().iterator();
		
		while( nodeIdIter.hasNext() )
		{
			overlapNodeIds.add(nodeIdIter.next());
		}
		
		return overlapNodeIds;
	}
	
	
	@Override
	public List<Integer> getNodeIDsForSearch
					(HashMap<String, AttributeValueRange> attrValRangeMap) 
	{
		// map so that we remove duplicates.
		//FIXME: this code is copied in many policies need to find a way to not copy code.
		HashMap<Integer, Integer> overlapNodeIdsMap = new HashMap<Integer, Integer>();
		
		for( int i=0; i<regionList.size(); i++ )
		{
			RegionInfo currRegion = regionList.get(i);
			ValueSpaceInfo regionValSpace = currRegion.getValueSpaceInfo();
			
			boolean overlap = true;
			
			Iterator<String> inputAttrIter = attrValRangeMap.keySet().iterator();
			
			while( inputAttrIter.hasNext() )
			{
				String attrName = inputAttrIter.next();
				
				AttributeMetaInfo attrMetaInfo = attributeMap.get(attrName);
				
				AttributeValueRange inputAttrValRange  
									= attrValRangeMap.get(attrName);
				
				AttributeValueRange regionAttrValRange 
								= regionValSpace.getValueSpaceBoundary().get(attrName);
				
				
				overlap = overlap && AttributeTypes.checkOverlapOfTwoIntervals(inputAttrValRange, 
												regionAttrValRange, attrMetaInfo.getDataType());
				
				if(!overlap)
				{
					break;
				}
			}
			
			if( overlap )
			{
				// Current region's value space overlaps with the value space in 
				// the input
				List<Integer> regionNodeList = currRegion.getNodeList();
				int randNodeId = regionNodeList.get
								(randGen.nextInt(regionNodeList.size()));
				overlapNodeIdsMap.put(randNodeId, randNodeId );
			}
		}
		
		List<Integer> overlapNodeIds = new LinkedList<Integer>();
		Iterator<Integer> nodeIdIter = overlapNodeIdsMap.keySet().iterator();
		
		while( nodeIdIter.hasNext() )
		{
			overlapNodeIds.add(nodeIdIter.next());
		}
		
		return overlapNodeIds;
	}
	
	
	@Override
	public void computeRegionMapping()
	{
		//FIXME: need to check if we need ceil of floor here.
		double numRegions = Math.ceil( Math.sqrt(nodeConfig.getNodeIDs().size()) );
		ValueSpaceInfo valSpaceInfo = new ValueSpaceInfo();
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
			valSpaceInfo.getValueSpaceBoundary().put(attrName, attrValRange);
		}
		
		double logVolumeOfValSpace = computeLogVolume(valSpaceInfo.getValueSpaceBoundary(), attributeMap);
		System.out.println("Total log volume of value space "+logVolumeOfValSpace);
		
		double desiredVolumeOfEachRegion = Math.log(1.0/numRegions) + logVolumeOfValSpace;
		System.out.println("Desired log volume of a region "+desiredVolumeOfEachRegion);
		
		
		int curr = 0;
		// numRegions -1 because the last remaining value space will be
		// numRegions th region.
		while(curr < (numRegions -1) )
		{
			String hyperplaneAttr = attrList.get(curr%attrList.size());
			RegionInfo regionInfo = partitionValueSpaceUsingHyperplane
					(valSpaceInfo, hyperplaneAttr, desiredVolumeOfEachRegion, attributeMap);
			
			regionList.add(regionInfo);
			curr++;
		}
		// remaining valuespace is the last region.
		RegionInfo regionInfo = new RegionInfo();
		regionInfo.setValueSpaceInfo(valSpaceInfo);
		regionList.add(regionInfo);
		
		
		// print regions.
		for(int i=0; i<regionList.size(); i++)
		{
			regionInfo = regionList.get(i);
			double volume = computeLogVolume(regionInfo.getValueSpaceInfo().getValueSpaceBoundary(), 
						attributeMap );
			System.out.println("Region num "+i+" log volume "+volume+" "+regionInfo.toString());
		}
		
		assignNodesUniformly();
	}
	
	
	/**
	 * Partitions the value space using a hyperplane on hyperplaneAttrName to create a region
	 * of volume desiredVolumeInLog. The remaining value space is updated in input parameter 
	 * valueSpace and the newly created region is returned.
	 * 
	 * @param valueSpace
	 * @param hyperplaneAttrName
	 * @param desiredVolumeInLog
	 * @return
	 */
	private  RegionInfo partitionValueSpaceUsingHyperplane
				( ValueSpaceInfo valueSpace, String hyperplaneAttrName, 
					double desiredVolumeInLog,  HashMap<String, AttributeMetaInfo> attributeMap )
	{
		// Valuespace is of the form , [ (a1, [low1, high1]), (a2, [low2, high2]), ... , (am, [lowm, highm]) ]
		// Let's say we want split attribute a1 with a hyperplane to get a region with the desiredVolumeInlog.
		// Let's say we split a1 at x, and we need to find x .
		// The new region is [ (a1, [low1, x]), (a2, [low2, high2]), ... , (am, [lowm, highm]) ].
		// So we compute the volume of the region above and equate it to desiredVolume to find x.
		// log(x-low1) + log(high2-low2) + log(hign3-low3) ... + log(highm-lowm) = desiredVolumeInLog.	
		
		HashMap<String, AttributeValueRange> regionExcludingAttr 
							= new HashMap<String, AttributeValueRange>();
		
		HashMap<String, AttributeValueRange> valSpaceBoundary = valueSpace.getValueSpaceBoundary();
		
		Iterator<String> attrIter = valSpaceBoundary.keySet().iterator();
		
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			
			if( attrName.equals(hyperplaneAttrName) )
			{
				// we are excluding hyperplaneAttrName attribute 
			}
			else
			{		
				regionExcludingAttr.put(attrName, valSpaceBoundary.get(attrName));
			}
		}
		
		// we compute partial volume on regionExcludingAttr attr
		
		double partialVol = computeLogVolume(regionExcludingAttr, attributeMap);
		
		double remVol = desiredVolumeInLog - partialVol;
		
		assert(remVol > 0);
		
		AttributeMetaInfo attrMetaInfo = attributeMap.get(hyperplaneAttrName);
		
		// FIXME: will fail in String data type
		if( attrMetaInfo.getDataType() == AttributeTypes.StringType )
		{
			assert(false);
		}
		
		AttributeValueRange attrValRange = valSpaceBoundary.get(hyperplaneAttrName);
		
		
		double lowerBoundD = Double.parseDouble( attrValRange.getLowerBound() );
		
		double upperBoundD = Double.parseDouble( attrValRange.getUpperBound() );
		
		// this condition is because we consider region to be at a1 < x not a1 >= x
		double x = Math.exp(remVol) + lowerBoundD;
		
		assert( (x > lowerBoundD) && (x < upperBoundD) );
		
		attrIter = valSpaceBoundary.keySet().iterator();
		
		ValueSpaceInfo regionValSpace = new ValueSpaceInfo();
		
		while( attrIter.hasNext() )
		{
			String currAttrName = attrIter.next();
			AttributeValueRange currAttrValRange = valSpaceBoundary.get(currAttrName);
			
			if( currAttrName.equals(hyperplaneAttrName) )
			{
				AttributeValueRange regionAttrValRange = new AttributeValueRange( 
							currAttrValRange.getLowerBound(), x+"" );
				
				AttributeValueRange spaceAttrValRange = new AttributeValueRange( 
						x+"", currAttrValRange.getUpperBound() );
				
				regionValSpace.getValueSpaceBoundary().put(currAttrName, regionAttrValRange);
				valSpaceBoundary.put(currAttrName, spaceAttrValRange);
			}
			else
			{
				// making duplicate copies so that both don't share pointers to same copy.
				AttributeValueRange regionAttrValRange = new AttributeValueRange( 
							currAttrValRange.getLowerBound(), currAttrValRange.getUpperBound() );
				
				regionValSpace.getValueSpaceBoundary().put(currAttrName, regionAttrValRange);
			}
		}
		
		RegionInfo regionInfo = new RegionInfo();
		regionInfo.setValueSpaceInfo(regionValSpace);
		return regionInfo;
	}
	
	
	/**
	 * Computes the volume of region in log scale.
	 * Volume of high dimensional space could be large so taking log
	 * @return
	 */
	private double computeLogVolume(HashMap<String, AttributeValueRange> valueSpaceBoundary, 
			HashMap<String, AttributeMetaInfo> attributeMap )
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
	
	private void assignNodesUniformly()
	{
		int currRegionIndex = 0;
		Iterator<Integer> nodeIdIter = nodeConfig.getNodeIDs().iterator();
		
		while( nodeIdIter.hasNext() )
		{
			int nodeId = nodeIdIter.next();
			
			RegionInfo regionInfo = regionList.get(currRegionIndex);
			
			if( regionInfo.getNodeList() == null )
			{
				List<Integer> nodeList = new LinkedList<Integer>();
				nodeList.add(nodeId);
				regionInfo.setNodeList(nodeList);
			}
			else
			{
				regionInfo.getNodeList().add(nodeId);
			}
			
			currRegionIndex++;
			currRegionIndex = currRegionIndex%regionList.size();
		}
	}
	
	
	public static void main(String[] args)
	{
		int NUM_ATTRS = 4;
		int NUM_NODES = 100;
		
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
		
		UniformGreedyRegionMappingPolicy obj = new UniformGreedyRegionMappingPolicy
														(givenMap, csNodeConfig);
		obj.computeRegionMapping();
	}
}