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
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;
import edu.umass.cs.contextservice.utils.Utils;

public class SqrtNConsistentHashingPolicy extends AbstractRegionMappingPolicy
{
	private HashMap<Integer, List<Integer>> nodePartitionMap;
	private final Random rand;
	private final int numPartitions;
	
	public SqrtNConsistentHashingPolicy(HashMap<String, AttributeMetaInfo> attributeMap, 
									CSNodeConfig nodeConfig) 
	{
		super(attributeMap, nodeConfig);
		
		nodePartitionMap = new HashMap<Integer, List<Integer>>();
		rand = new Random();
		numPartitions = (int) Math.floor(Math.sqrt(nodeConfig.getNodes().size()));
	}

	@Override
	public List<Integer> getNodeIDsForAValueSpaceForSearch(ValueSpaceInfo valueSpace) 
	{
		int parNum = rand.nextInt(numPartitions);
		// assuming caller won't tamper with the list.
		return nodePartitionMap.get(parNum);
	}

	@Override
	public List<Integer> getNodeIDsForAValueSpaceForUpdate(String GUID, 
					ValueSpaceInfo valueSpace) 
	{
		List<Integer> nodeList = new LinkedList<Integer>();
		
		Iterator<Integer> partitionIter = nodePartitionMap.keySet().iterator();
		
		while(partitionIter.hasNext())
		{
			int partNum = partitionIter.next();
			List<Integer> partNodeList = nodePartitionMap.get(partNum);
			int hashIndex = Utils.consistentHashAString(GUID, partNodeList.size());
			nodeList.add(partNodeList.get(hashIndex));
		}
		
		return nodeList;
	}

	@Override
	public void computeRegionMapping() 
	{	
		Iterator<Integer> nodeIter = nodeConfig.getNodeIDs().iterator();
		
		int currPart = 0;
		while(nodeIter.hasNext())
		{
			int nodeid = nodeIter.next();
			List<Integer> nodeList = nodePartitionMap.get(currPart);
			if(nodeList == null)
			{
				nodeList = new LinkedList<Integer>();
				nodeList.add(nodeid);
				nodePartitionMap.put(currPart, nodeList);
			}
			else
			{
				nodeList.add(nodeid);
			}
			currPart++;
			currPart = currPart%numPartitions;
		}
		System.out.println("nodePartitionMap "+nodePartitionMap);
	}
	
	
	public static void main(String[] args)
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
						new InetSocketAddress(InetAddress.getByName("localhost"), 3000+i));
			}
			catch (UnknownHostException e)
			{
				e.printStackTrace();
			}
		}
		
		AttributeTypes.initializeGivenMapAndList(givenMap, attrList);
		
		SqrtNConsistentHashingPolicy obj 
				= new SqrtNConsistentHashingPolicy(givenMap, csNodeConfig);
		
		obj.computeRegionMapping();
		
		
		List<Integer> searchList = obj.getNodeIDsForAValueSpaceForSearch(null);
		System.out.println("searchList "+searchList);
		
		
		List<Integer> updateList = obj.getNodeIDsForAValueSpaceForUpdate("123", null);
		System.out.println("updateList "+updateList);

		
		updateList = obj.getNodeIDsForAValueSpaceForUpdate("456", null);
		System.out.println("updateList "+updateList);
		
	}
}