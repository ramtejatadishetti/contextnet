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

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.datasource.AbstractDataSource;
import edu.umass.cs.contextservice.database.datasource.SQLiteDataSource;
import edu.umass.cs.contextservice.regionmapper.database.AbstractRegionMappingStorage;
import edu.umass.cs.contextservice.regionmapper.database.SQLRegionMappingStorage;
import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;
import edu.umass.cs.contextservice.regionmapper.helper.RegionInfo;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;

/**
 * This policy reads regions from a file. 
 * The file should be in location ContextServiceConfig.configFileDirectory+
				"/"+ContextServiceConfig.REGION_INFO_FILENAME along with other 
				node , db and csConfig files.			
 * @author ayadav
 */
public class FileBasedRegionMappingPolicyWithDB extends AbstractRegionMappingPolicy
{
	private final HashMap<Integer, RegionInfo> regionMap;
	private final Random randGen;
	
	private final AbstractRegionMappingStorage regionMappingStorage;
	
	public FileBasedRegionMappingPolicyWithDB( AbstractDataSource dataSource,
			HashMap<String, AttributeMetaInfo> attributeMap, 
			CSNodeConfig nodeConfig )
	{
		super(attributeMap, nodeConfig);
		regionMap = new HashMap<Integer, RegionInfo>();
		randGen = new Random();
		regionMappingStorage = new SQLRegionMappingStorage(dataSource, attributeMap);
		
		regionMappingStorage.createTables();
	}
	
	
	@Override
	public List<Integer> getNodeIDsForAValueSpaceForSearch(ValueSpaceInfo valueSpace)
	{
		// map so that we remove duplicates.
		HashMap<Integer, Integer> overlapNodeIdsMap = new HashMap<Integer, Integer>();
				
		List<Integer> regionKeyList = regionMappingStorage.getNodeIdsForValueSpace
								(ContextServiceConfig.REGION_INFO_TABLE_NAME, valueSpace).get(0);
		
		for(int i=0; i<regionKeyList.size(); i++)
		{
			RegionInfo overlapRegion = regionMap.get(regionKeyList.get(i));
			
			List<Integer> regionNodeList = overlapRegion.getNodeList();
			int randNodeId 
					= regionNodeList.get(randGen.nextInt(regionNodeList.size()));
			overlapNodeIdsMap.put(randNodeId, randNodeId );
		}
		
		
		List<Integer> overlapNodeIds = new LinkedList<Integer>();
		Iterator<Integer> nodeIdIter = overlapNodeIdsMap.keySet().iterator();
		
		while( nodeIdIter.hasNext() )
		{
			overlapNodeIds.add(nodeIdIter.next());
		}
		
		assert(overlapNodeIds.size() >= 1);
		return overlapNodeIds;
	}
	
	
	@Override
	public List<Integer> getNodeIDsForAValueSpaceForUpdate(
			String GUID, ValueSpaceInfo valueSpace)
	{	
		// map so that we remove duplicates.
		HashMap<Integer, Integer> overlapNodeIdsMap = new HashMap<Integer, Integer>();
		
		List<Integer> regionKeyList = regionMappingStorage.getNodeIdsForValueSpace
				(ContextServiceConfig.REGION_INFO_TABLE_NAME, valueSpace).get(0);
		
		for(int i=0; i<regionKeyList.size(); i++)
		{
			RegionInfo overlapRegion = regionMap.get(regionKeyList.get(i));
			
			List<Integer> regionNodeList = overlapRegion.getNodeList();
			
			for( int j=0; j<regionNodeList.size(); j++ )
			{
				overlapNodeIdsMap.put(regionNodeList.get(j), 
							regionNodeList.get(j) );
			}	
		}
		
		List<Integer> overlapNodeIds = new LinkedList<Integer>();
		Iterator<Integer> nodeIdIter = overlapNodeIdsMap.keySet().iterator();
		
		while( nodeIdIter.hasNext() )
		{
			overlapNodeIds.add(nodeIdIter.next());
		}	
		assert(overlapNodeIds.size() >= 1);
		return overlapNodeIds;
	}
	
	
	@Override
	public void computeRegionMapping() 
	{
		BufferedReader br 	= null;
		FileReader fr 		= null;
		
		try
		{
			fr = new FileReader(ContextServiceConfig.configFileDirectory+
					"/"+ContextServiceConfig.REGION_INFO_FILENAME);
			br = new BufferedReader(fr);
			
			String valSpaceString;
			int regionKey = 0 ;
			while( (valSpaceString = br.readLine()) != null )
			{
				ValueSpaceInfo valSpace 
								= ValueSpaceInfo.fromString(valSpaceString);
				RegionInfo regionInfo 
								= new RegionInfo();
				
				regionInfo.setValueSpaceInfo(valSpace);
				regionInfo.setRegionKey(regionKey);
				regionMap.put(regionKey, regionInfo);
				regionKey++;
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
		
		// store region in db.
		Iterator<Integer> regionKeyIter = regionMap.keySet().iterator();
		
		while(regionKeyIter.hasNext())
		{
			int regionKey = regionKeyIter.next();
			RegionInfo regionInfo = regionMap.get(regionKey);
			regionMappingStorage.insertRegionInfoIntoTable(
						ContextServiceConfig.REGION_INFO_TABLE_NAME, regionInfo);
		}
	}
	
	
	private void assignNodesUniformly()
	{
		//FIXME: currently key is index the 
		// implementation might need to change.
		
		int currRegionIndex = 0;
		Iterator<Integer> nodeIdIter = nodeConfig.getNodeIDs().iterator();
		
		while( nodeIdIter.hasNext() )
		{
			int nodeId = nodeIdIter.next();
			
			RegionInfo regionInfo = regionMap.get(currRegionIndex);
			
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
			currRegionIndex = currRegionIndex%regionMap.size();
		}
	}
	
	public static void main(String[] args) throws PropertyVetoException
	{
		int NUM_ATTRS = Integer.parseInt(args[0]);
		int NUM_NODES = Integer.parseInt(args[1]);
		
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
						new InetSocketAddress(InetAddress.getByName
								("localhost"), 3000+i));
			}
			catch (UnknownHostException e)
			{
				e.printStackTrace();
			}
		}
		AttributeTypes.initializeGivenMapAndList(givenMap, attrList);
		
		AbstractRegionMappingPolicy regionMapping 
				= new FileBasedRegionMappingPolicyWithDB(new SQLiteDataSource(0), givenMap, 
						csNodeConfig);	
		
		regionMapping.computeRegionMapping();
		
		// example value space
		ValueSpaceInfo vspaceInfo = new ValueSpaceInfo();
		vspaceInfo.getValueSpaceBoundary().put("attr10", 
						new AttributeValueRange(1+"", 1500+""));
		
		List<Integer> nodeList = regionMapping.getNodeIDsForAValueSpaceForSearch
						(vspaceInfo);
		
		System.out.println("Node list size "+nodeList.size()+" expected "+NUM_NODES);
		
		assert(nodeList.size() == NUM_NODES);
	}
}