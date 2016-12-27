package edu.umass.cs.contextservice.queryparsing;




/**
 * lower level QueryInfo, used to pass information for porcessing during 
 * query processing. Query components are processed and converted
 * into a simple ProcessingQueryComponents that are used for hyperspace processing.
 * ProcessingQueryComponent is a simpler range or equality predicates
 * @author adipc
 *
 */
public class ProcessingQueryComponentOld 
{	
	// macros as JSON Keys
	public static final String AttrName					= "ATTR_NAME";
	//public static final String LeftOper				= "LEFT_OPER";
	//public static final String LeftValue				= "LEFT_VAL";
	//public static final String RightOper				= "RIGHT_OPER";
	//public static final String RightVal					= "RIGHT_VAL";
	public static final String CompId					= "COMP_ID";
	public static final String NumRepForComp			= "NUM_REP_COMP";
	
	
	// represents  leftValue leftOperator attributeName rightOperator rightValue, like 10 <= a <= 20
	private  String attributeName;
	//private  String leftOperator;
	private  String lowerBound;
	//private  String rightOperator;
	private  String upperBound;
	
	// A query is split into many components, each component within
	// a query has a unique ID.
	private int componentID;
	
	// indicates the number of replies received for this component
	private int numCompReplyRecvd = 0;
	
	// indicates the total number of replies received for this component
	private int totalCompReply;
	
	// new definitions
	
	// whether it is a function or a comparison operator predicate
	// all the values will be stored in String and will be converted on
	// fly based on the datatype

	
	public ProcessingQueryComponentOld( String attributeName, String lowerBound, String upperBound)
	{
		this.attributeName = attributeName;
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
	}
	
	public String getAttributeName()
	{
		return attributeName;
	}
	
	public String getLowerBound()
	{
		return lowerBound;
	}
	
	public String getUpperBound()
	{
		return upperBound;
	}
	
	public void setLowerBound(String lowerBound)
	{
		this.lowerBound = lowerBound;
	}
	
	public void setUpperBound(String upperBound)
	{
		this.upperBound = upperBound;
	}
	
	public void setComponentID(int componentID)
	{
		this.componentID = componentID;
	}
	
	public int getComponentID()
	{
		return this.componentID;
	}
	
	public void setTotalCompReply(int totalCompReply)
	{
		this.totalCompReply = totalCompReply;
	}
	
	public synchronized void updateNumCompReplyRecvd()
	{
		this.numCompReplyRecvd++;
	}
	
	public int getTotalCompReply()
	{
		return totalCompReply;
	}
	
	public int getNumCompReplyRecvd()
	{
		return numCompReplyRecvd;
	}
	
	/**
	 * Converts the predicate back into 
	 * the query format.
	 * Needed in privacy case when a query is parsed
	 * at the client and sent based on privacy mechanism.
	 */
	public String toString()
	{
		String str = this.attributeName +" >= "+lowerBound+" AND "+
				this.attributeName+" <= "+upperBound;
		return str;
	}
}