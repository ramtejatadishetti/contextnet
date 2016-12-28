package edu.umass.cs.contextservice.regionmapper;

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

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;
import edu.umass.cs.contextservice.regionmapper.helper.RegionInfo;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;

/**
 * This policy reads regions from a file. The fullpath
 * name should be given as input.
 * 
 * @author ayadav
 */
public class FileBasedRegionMappingPolicy extends AbstractRegionMappingPolicy
{
	private final String fileName;
	private final List<RegionInfo> regionList;
	private final Random randGen;
	
	public FileBasedRegionMappingPolicy( HashMap<String, AttributeMetaInfo> attributeMap, 
			CSNodeConfig nodeConfig, String fileName )
	{
		super(attributeMap, nodeConfig);
		this.fileName = fileName;
		regionList = new LinkedList<RegionInfo>();
		randGen = new Random();
	}
	
	
	@Override
	public List<Integer> getNodeIDsForAValueSpace(ValueSpaceInfo valueSpace, 
					REQUEST_TYPE requestType)
	{
		// map so that we remove duplicates.
		HashMap<Integer, Integer> overlapNodeIdsMap = new HashMap<Integer, Integer>();
		
		for( int i=0; i<regionList.size(); i++ )
		{
			RegionInfo currRegion = regionList.get(i);
			ValueSpaceInfo regionValSpace = currRegion.getValueSpaceInfo();
			
			boolean overlap = true;
			
			Iterator<String> inputAttrIter = valueSpace.getValueSpaceBoundary().keySet().iterator();
			
			while( inputAttrIter.hasNext() )
			{
				String attrName = inputAttrIter.next();
				
				AttributeMetaInfo attrMetaInfo = attributeMap.get(attrName);
				
				AttributeValueRange inputAttrValRange  
									= valueSpace.getValueSpaceBoundary().get(attrName);
				
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
				if(requestType == REQUEST_TYPE.UPDATE)
				{
					// Current region's value space overlaps with the value space in 
					// the input
					List<Integer> regionNodeList = currRegion.getNodeList();
					
					for( int j=0; j<regionNodeList.size(); j++ )
					{
						overlapNodeIdsMap.put(regionNodeList.get(j), regionNodeList.get(j) );
					}
				}
				else if(requestType == REQUEST_TYPE.SEARCH)
				{
					// Current region's value space overlaps with the value space in 
					// the input
					List<Integer> regionNodeList = currRegion.getNodeList();
					int randNodeId = regionNodeList.get(randGen.nextInt(regionNodeList.size()));
					overlapNodeIdsMap.put(randNodeId, randNodeId );
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
	public void computeRegionMapping() 
	{
		BufferedReader br 	= null;
		FileReader fr 		= null;
		
		try
		{
			fr = new FileReader(fileName);
			br = new BufferedReader(fr);
			
			String valSpaceString;
			
			while( (valSpaceString = br.readLine()) != null )
			{
				ValueSpaceInfo valSpace 
								= ValueSpaceInfo.fromString(valSpaceString);
				RegionInfo regionInfo 
								= new RegionInfo();
				
				regionInfo.setValueSpaceInfo(valSpace);
				regionList.add(regionInfo);
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
		
		assignNodesUniformly();
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
		
		
		String filename = "RegionInfoNumNodes100.txt";
		AbstractRegionMappingPolicy regionMapping 
				= new FileBasedRegionMappingPolicy(givenMap, 
						csNodeConfig, filename);
		
		
		regionMapping.computeRegionMapping();
		
		// example value space
		ValueSpaceInfo vspaceInfo = new ValueSpaceInfo();
		vspaceInfo.getValueSpaceBoundary().put("attr10", new AttributeValueRange(1+"", 1500+""));
		
		List<Integer> nodeList = regionMapping.getNodeIDsForAValueSpace(vspaceInfo, 
				REQUEST_TYPE.SEARCH);
		
		System.out.println("Node list size "+nodeList.size()+" expected "+NUM_NODES);
		
		assert(nodeList.size() == NUM_NODES);
	}
}