package edu.umass.cs.contextservice.queryparsing.functions;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;

public abstract class AbstractFunction
{
	// function name and class name
	public static final Map<String, Class<?>> registeredFunctionsMap = new HashMap<String, Class<?>>();
	static
	{
		registeredFunctionsMap.put("Overlap", OverlapFunction.class);
		registeredFunctionsMap.put("Distance", DistanceFunction.class);
		registeredFunctionsMap.put("GeojsonOverlap", GeojsonOverlapFunction.class);
	}
	    
	protected final String funcName;
	
	protected final String[] funcArguments;
	
	// attribute accessed in the function
	// usually it is just one, but geoJSON
	// has both latitude and longitude accessed in one function, 
	// so a list
	protected List<String> attrList;
	
	public AbstractFunction( String funcName, String[] funcArguments )
	{
		this.funcName = funcName;
		this.funcArguments = funcArguments;
		attrList = new LinkedList<String>();
	}
	
	public String getFunctionName()
	{
		return funcName;
	}
	
	public String[] getArgs()
	{
		return funcArguments;
	}
	
	public abstract Vector<ProcessingQueryComponent> getProcessingQueryComponents();
	
	/**
	 * checks a db resultset against the function 
	 * to check if the function satisfies the given record
	 * @param rs
	 * @return
	 */
	public abstract boolean checkDBRecordAgaistFunction(ResultSet rs);
}