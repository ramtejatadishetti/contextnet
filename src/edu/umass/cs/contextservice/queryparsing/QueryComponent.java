package edu.umass.cs.contextservice.queryparsing;

import java.lang.reflect.InvocationTargetException;

import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.queryparsing.functions.AbstractFunction;

/**
 * Stores the components of the query
 * @author ayadav
 */
public class QueryComponent
{
	// whether predicate is based on comparison operator 
	// or an already registered function
	public static final int COMPARISON_PREDICATE		= 1;
	public static final int FUNCTION_PREDICATE			= 2;
	public static final int JOIN_INFO					= 3;
	
	
	// macros as JSON Keys
	/*public static final String AttrName					= "ATTR_NAME";
	public static final String LeftOper					= "LEFT_OPER";
	public static final String LeftValue				= "LEFT_VAL";
	public static final String RightOper				= "RIGHT_OPER";
	public static final String RightVal					= "RIGHT_VAL";
	public static final String CompId					= "COMP_ID";
	public static final String NumRepForComp			= "NUM_REP_COMP";*/

	
	// represents  leftValue leftOperator attributeName rightOperator rightValue, like 10 <= a <= 20
	
	/*private  String leftOperator;
	private  double leftValue;
	private  String rightOperator;
	private  double rightValue;*/
	
	// A query is split into many components, each component within
	// a query has a unique ID.
	//private int componentID;
	
	// indicates the number of replies received for this component
	//private int numCompReplyRecvd = 0;
	
	// indicates the total number of replies received for this component
	//private int totalCompReply;
	
	//private List<MetadataTableInfo<Integer>> qcMetadataInfo;
	
	// new definitions
	private  String attributeName;
	// whether it is a function or a comparison operator predicate
	// all the values will be stored in String and will be converted on
	// fly based on the datatype
	// comparison operator things
	private int typeOfComponent;
	
	private  String operator;
	private  String value;
	
	
	// function predicate things
	private AbstractFunction functionObj;
	
	
	// join things
	private String[] joinGUIDList;
	
	/*public QueryComponent( String attributeName, String leftOperator, double leftValue, 
			String rightOperator, double rightValue )
	{
		this.attributeName = attributeName;
		this.leftOperator = leftOperator;
		this.leftValue = leftValue;
		this.rightOperator = rightOperator;
		this.rightValue = rightValue;
	}*/
	
	public QueryComponent(int typeOfComponent, String attrName, String operator, String value)
	{
		this.typeOfComponent = typeOfComponent;
		this.attributeName   = attrName;
		this.value = value;
		this.operator = operator;
		ContextServiceLogger.getLogger().fine("Where cons "+typeOfComponent+" "+attributeName+" "+value+" oper "+operator);
	}
	
	public QueryComponent(int typeOfComponent, String functionName, String[] args)
	{
		this.typeOfComponent = typeOfComponent;
		// if functions are added dynamically then
		// this switch case can be replaced with reflection
		
		this.functionObj = getFunctionObject(functionName, args);
		
		/*this.funcName   = functionName;
		this.funcArguments = args;
		
		System.out.print("Function cons "+typeOfComponent+" "+functionName);
		for(int i=0;i<funcArguments.length;i++)
		{
			System.out.print(funcArguments[i]);
		}*/
	}
	
	private  AbstractFunction getFunctionObject(String functionName, String[] args)
	{
		AbstractFunction funObject = null;
		
		try
		{
			//if(csType!=null && getPacketTypeClassName(csType)!=null) 
			if(AbstractFunction.registeredFunctionsMap.containsKey(functionName))
			{
				String classSimpleName = AbstractFunction.registeredFunctionsMap.get(functionName).getSimpleName();
				
				funObject = (AbstractFunction)(Class.forName(
						"edu.umass.cs.contextservice.queryparsing.functions." + classSimpleName).getConstructor
						(String.class, String[].class).newInstance(functionName, args));
			}
		}
		catch(NoSuchMethodException nsme) {nsme.printStackTrace();} 
		catch(InvocationTargetException ite) {ite.printStackTrace();} 
		catch(IllegalAccessException iae) {iae.printStackTrace();}
		catch(ClassNotFoundException cnfe) {cnfe.printStackTrace();}
		catch(InstantiationException ie) {ie.printStackTrace();}
		return funObject;
	}
	
	public String getAttributeName()
	{
		return attributeName;
	}
	
	public int getComponentType()
	{
		return this.typeOfComponent;
	}
	
	public String getOperator()
	{
		return this.operator;
	}
	
	public String getValue()
	{
		return this.value;
	}
	
	public AbstractFunction getFunction()
	{
		return this.functionObj;
	}
	
	/*public double getLeftValue()
	{
		return leftValue;
	}
	
	public double getRightValue()
	{
		return rightValue;
	}*/
	
	/*public void setComponentID(int componentID)
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
	}*/
	
	/*public void setValueNodeArray(List<MetadataTableInfo<Integer>> qcMetadataInfo)
	{
		this.qcMetadataInfo = qcMetadataInfo;
	}*/
	
	/*public JSONObject getJSONObject() throws JSONException
	{
		JSONObject qcJSON = new JSONObject();
		qcJSON.put(QueryComponent.AttrName, this.attributeName);
		qcJSON.put(QueryComponent.LeftOper, this.leftOperator);
		qcJSON.put(QueryComponent.LeftValue, this.leftValue);
		qcJSON.put(QueryComponent.RightOper, this.rightOperator);
		qcJSON.put(QueryComponent.RightVal, this.rightValue);
		qcJSON.put(QueryComponent.CompId, this.componentID);
		return qcJSON;
	}
	
	public static QueryComponent getQueryComponent(JSONObject jobj) throws JSONException
	{
		QueryComponent qc = new QueryComponent(jobj.getString(QueryComponent.AttrName), 
				jobj.getString(QueryComponent.LeftOper), jobj.getDouble(QueryComponent.LeftValue), 
				jobj.getString(QueryComponent.RightOper), jobj.getDouble(QueryComponent.RightVal));
		qc.setComponentID(jobj.getInt(QueryComponent.CompId));	
		return qc;
	}
	
	public String toString()
	{
		return this.leftValue + " " + this.leftOperator
				+ " " + this.attributeName + " " + this.rightOperator + " " + this.rightValue;
	}*/
	
	/**
	 * Function checks if the parameter value 
	 * satisfies the given component, returns true otherwise false
	 * right now we just have comparison operator,
	 * this function will change as we have more operators,
	 * but each component(predicate) in a conjunction can be checked 
	 * by this function
	 * @return
	 */
	/*public boolean checkIfValueSatisfiesComponent( JSONObject attrValueJSON )
	{
		try 
		{
			double attrVal = attrValueJSON.getDouble(attributeName);
			// just for these operators right now
			if( (attrVal >= leftValue) && (attrVal < rightValue) )
			{
				return true;
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		return false;
	}*/
}