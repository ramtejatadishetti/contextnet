package edu.umass.cs.contextservice.database.inmemorydb;

public class AttributePartition<DataType>
{
	String attrName;
	DataType lowerBound;
	DataType upperBound;
}