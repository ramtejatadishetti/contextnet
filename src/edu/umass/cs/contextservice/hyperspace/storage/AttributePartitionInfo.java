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
	public void initializePartitionInfo( int numPartitions, int defaultPartitionNum )
	{
		this.numPartitions = numPartitions;
		assert(this.attrMetaInfo != null);
		
		domainParitionInfo = AttributeTypes.partitionDomain(numPartitions, attrMetaInfo.getMinValue(), 
				attrMetaInfo.getMaxValue(), attrMetaInfo.getDataType());
		//System.out.println(" numPartitions "+numPartitions+" defaultPartitionNum "+defaultPartitionNum+" size "+domainParitionInfo.size());
		assert( defaultPartitionNum < domainParitionInfo.size() );
		assert( defaultPartitionNum == domainParitionInfo.get(defaultPartitionNum).partitionNum );
		partitionDefaultValue = domainParitionInfo.get(defaultPartitionNum).lowerbound;
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
	
	public String getDefaultValue()
	{
		return this.partitionDefaultValue;
	}
}