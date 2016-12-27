package edu.umass.cs.contextservice.queryparsing;

/**
 * Stores the components of the query
 * @author ayadav
 */
public class QueryComponentOld
{
	// new definitions
	private  String attributeName;
	
	private  String operator;
	private  String value;
	
	public QueryComponentOld(String attrName, String operator, String value)
	{
		this.attributeName   = attrName;
		this.value = value;
		this.operator = operator;
	}
	
	public String getAttributeName()
	{
		return attributeName;
	}
	
	public String getOperator()
	{
		return this.operator;
	}
	
	public String getValue()
	{
		return this.value;
	}
}