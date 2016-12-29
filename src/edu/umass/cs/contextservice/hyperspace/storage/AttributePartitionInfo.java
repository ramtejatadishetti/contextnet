package edu.umass.cs.contextservice.hyperspace.storage;

import java.util.List;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes.RangePartitionInfo;

public class AttributePartitionInfo
{
	private final AttributeMetaInfo attrMetaInfo;
	
	// for hyerpspace hashing subspaces;
	private int subspaceNumPartitions;
	private List<RangePartitionInfo> subspaceDomainParitionInfo;
	
	public AttributePartitionInfo(AttributeMetaInfo attrMetaInfo )
	{
		this.attrMetaInfo = attrMetaInfo;
	}
	
	/**
	 * Initializes the partition info and sets the default value for this attribute
	 * @param numPartitions
	 */
	public void initializePartitionInfo( int numSubspacePartitions)
	{
		assert(numSubspacePartitions > 0 );
		
		this.subspaceNumPartitions = numSubspacePartitions;
		
		assert(this.attrMetaInfo != null);
		
		subspaceDomainParitionInfo = AttributeTypes.partitionDomain
				(subspaceNumPartitions, attrMetaInfo.getMinValue(), 
				attrMetaInfo.getMaxValue(), attrMetaInfo.getDataType());
	}
	
	
	public int getSubspaceNumPartitions()
	{
		return this.subspaceNumPartitions;
	}
	
	public AttributeMetaInfo getAttrMetaInfo()
	{
		return this.attrMetaInfo;
	}
	
	public List<RangePartitionInfo> getSubspaceDomainPartitionInfo()
	{
		return this.subspaceDomainParitionInfo;
	}
}