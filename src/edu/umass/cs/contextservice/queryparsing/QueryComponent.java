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
}