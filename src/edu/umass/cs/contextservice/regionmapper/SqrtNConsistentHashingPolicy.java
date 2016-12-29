package edu.umass.cs.contextservice.regionmapper;

import java.util.HashMap;
import java.util.List;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;

public class SqrtNConsistentHashingPolicy extends AbstractRegionMappingPolicy
{

	public SqrtNConsistentHashingPolicy(HashMap<String, AttributeMetaInfo> attributeMap, 
									CSNodeConfig nodeConfig) 
	{
		super(attributeMap, nodeConfig);
		
	}

	@Override
	public List<Integer> getNodeIDsForAValueSpaceForSearch(ValueSpaceInfo valueSpace) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Integer> getNodeIDsForAValueSpaceForUpdate(String GUID, ValueSpaceInfo valueSpace) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void computeRegionMapping() {
		// TODO Auto-generated method stub
		
	}
	
}