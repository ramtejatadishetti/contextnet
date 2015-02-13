package edu.umass.cs.contextservice;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import edu.umass.cs.contextservice.config.ContextServiceConfig;

/**
 * Defines the attribute types on which context based 
 * communication is supported.
 * @author ayadav
 *
 */
public class AttributeTypes
{
	//public static String [] attributeNames						= null;
	
	public static final double MIN_VALUE						= 1.0;
	public static final double MAX_VALUE						= 1500.0;
	
	public static final double NOT_SET							= Double.MIN_VALUE;
	
	private static Map<String, String> attributeMap 				= null;
	
	
	/**
	 * checks if the passed value is an attribute or not
	 * @param attribute
	 * @return true if it's a attribute
	 */
	public static boolean checkIfAttribute(String attribute)
	{
		/*if(attributeMap == null)
		{
			initialize();
		}*/
		return attributeMap.containsKey(attribute);
	}
	
	public static void initialize()
	{
		attributeMap = new HashMap<String, String>();
		for(int i=0;i<ContextServiceConfig.NUM_ATTRIBUTES;i++)
		{
			attributeMap.put(ContextServiceConfig.CONTEXT_ATTR_PREFIX+"ATT"+i,
					ContextServiceConfig.CONTEXT_ATTR_PREFIX+"ATT"+i);
		}
	}
	
	public static Vector<String> getAllAttributes()
	{
		Vector<String> attributes = new Vector<String>();
		attributes.addAll(attributeMap.keySet());
		return attributes;
	}
}