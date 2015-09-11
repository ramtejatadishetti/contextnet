package edu.umass.cs.contextservice.processing;

import java.util.Vector;

import edu.umass.cs.contextservice.AttributeTypes;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;

/**
 * 
 * Implements query parser
 * @author ayadav
 */
public class QueryParser
{
	// equal, less equal, greater equal, less, great, not
	public static String [] attributeOperators				= {"==", "<=", ">=", "<", ">", "!"};
	
	public static String [] booleanOperators				= {"&&"};
	
	/*public static void queryParsing(String query)
	{
		String [] queryParts = query.split(" ");
	}*/
	
	public static Vector<QueryComponent> parseQuery(String query)
	{
		Vector<QueryComponent> qcomponents = new Vector<QueryComponent>();
		
		String[] conjucSplit = query.split("&&");
		for(int i=0;i<conjucSplit.length;i++)
		{
			QueryComponent qc = getComponent(conjucSplit[i]);
			qc.setComponentID(i);
			qcomponents.add(qc);
		}
		return qcomponents;
	}
	
	private static QueryComponent getComponent(String predicate)
	{
		ContextServiceLogger.getLogger().fine("getComponent "+predicate);
		String[] predicateParts = predicate.split(" ");
		int attrIndex = -1;
		for(int i=0;i<predicateParts.length;i++)
		{
			if(AttributeTypes.checkIfAttribute(predicateParts[i]))
			{
				attrIndex = i;
				break;
			}
		}
		
		String leftOperator = null, rightOperator = null;
		double leftValue = 0, rightValue = 0;
		if( (attrIndex -1) >= 0 )
		{
			leftOperator = predicateParts[(attrIndex -1)];
		}
		
		if( (attrIndex -2) >= 0 )
		{
			leftValue = Double.parseDouble(predicateParts[(attrIndex -2)]);
		}
		
		if( (attrIndex + 1) <  predicateParts.length)
		{
			rightOperator = predicateParts[(attrIndex + 1)];
		}
		
		if( (attrIndex + 2) <  predicateParts.length)
		{
			rightValue = Double.parseDouble(predicateParts[(attrIndex + 2)]);
		}
		
		QueryComponent qc = new QueryComponent(predicateParts[attrIndex], leftOperator, 
				leftValue, rightOperator, rightValue);
		
		return qc;
	}
	
}