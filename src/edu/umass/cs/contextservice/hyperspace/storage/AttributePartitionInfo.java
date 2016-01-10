package edu.umass.cs.contextservice.hyperspace.storage;

import java.util.Vector;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes.DomainPartitionInfo;

public class AttributePartitionInfo
{
	private final AttributeMetaInfo attrMetaInfo;
	private int numPartitions;
	Vector<DomainPartitionInfo> domainParitionInfo;
	
	
	public AttributePartitionInfo(AttributeMetaInfo attrMetaInfo )
	{
		this.attrMetaInfo = attrMetaInfo;
	}
	
	public void initializePartitionInfo(int numPartitions)
	{
		this.numPartitions = numPartitions;
		assert(this.attrMetaInfo != null);
		
		domainParitionInfo = AttributeTypes.partitionDomain(numPartitions, attrMetaInfo.getMinValue(), 
				attrMetaInfo.getMaxValue(), attrMetaInfo.getDataType());
	}
	
	public int getNumPartitions()
	{
		return numPartitions;
	}
	
	public AttributeMetaInfo getAttrMetaInfo()
	{
		return this.attrMetaInfo;
	}
	
	public Vector<DomainPartitionInfo> getDomainPartitionInfo()
	{
		return this.domainParitionInfo;
	}
}