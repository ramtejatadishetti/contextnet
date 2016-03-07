package edu.umass.cs.contextservice.hyperspace.storage;

import java.util.Vector;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes.DomainPartitionInfo;

public class AttributePartitionInfo
{
	private final AttributeMetaInfo attrMetaInfo;
	
	// for hyerpspace hashing subspaces;
	private int subspaceNumPartitions;
	Vector<DomainPartitionInfo> subspaceDomainParitionInfo;
	
	// for triggers
	private int triggerNumPartitions;
	Vector<DomainPartitionInfo> triggerDomainParitionInfo;
	
	// default value of this attribute.
	// now default value is chosen uniformly and based on partition.
	// Like if there are 3 partitions of each attribute and and there 4 attributees
	// then the default value is chosen as follows. Attr1 default value = Lowervalue(Parition0)\
	// Attr2 default value = Lowervalue(Parition1) , Attr3 default value = Lowervalue(Parition2),
	// Attr4 default value = Lowervalue(Parition0).
	// So that the default value and especially in trigger case triggers are stored
	// uniformly across nodes.
	// This mechanism should also be specified in the paper draft.
	private String partitionDefaultValue;
	
	public AttributePartitionInfo(AttributeMetaInfo attrMetaInfo )
	{
		this.attrMetaInfo = attrMetaInfo;
	}
	
	/**
	 * Initializes the partition info and sets the default value for this attribute
	 * @param numPartitions
	 */
	public void initializePartitionInfo( int numSubspacePartitions, int triggerNumPartitions, 
			int defaultPartitionNum )
	{
		this.subspaceNumPartitions = numSubspacePartitions;
		this.triggerNumPartitions = triggerNumPartitions;
		
		assert(this.attrMetaInfo != null);
		
		subspaceDomainParitionInfo = AttributeTypes.partitionDomain(subspaceNumPartitions, attrMetaInfo.getMinValue(), 
				attrMetaInfo.getMaxValue(), attrMetaInfo.getDataType());
		
		triggerDomainParitionInfo = AttributeTypes.partitionDomain(triggerNumPartitions, attrMetaInfo.getMinValue(), 
				attrMetaInfo.getMaxValue(), attrMetaInfo.getDataType());
		
		//System.out.println(" numPartitions "+numPartitions+" defaultPartitionNum "+defaultPartitionNum+" size "+domainParitionInfo.size());
		
		// just ignoring defaultPartitionNum for a while. Did this for earlier trigger scheme, 
		// that scheme has changed so not sure if we need it now.
		defaultPartitionNum = 0;
		assert( defaultPartitionNum < subspaceDomainParitionInfo.size() );
		assert( defaultPartitionNum == subspaceDomainParitionInfo.get(defaultPartitionNum).partitionNum );
		partitionDefaultValue = subspaceDomainParitionInfo.get(defaultPartitionNum).lowerbound;
	}
	
	
	public int getSubspaceNumPartitions()
	{
		return this.subspaceNumPartitions;
	}
	
	public int getTriggerNumPartitions()
	{
		return this.triggerNumPartitions;
	}
	
	public AttributeMetaInfo getAttrMetaInfo()
	{
		return this.attrMetaInfo;
	}
	
	public Vector<DomainPartitionInfo> getSubspaceDomainPartitionInfo()
	{
		return this.subspaceDomainParitionInfo;
	}
	
	public Vector<DomainPartitionInfo> getTriggerDomainPartitionInfo()
	{
		return this.triggerDomainParitionInfo;
	}
	
	public String getDefaultValue()
	{
		return this.partitionDefaultValue;
	}
}