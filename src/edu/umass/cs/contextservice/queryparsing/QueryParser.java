package edu.umass.cs.contextservice.queryparsing;



import java.util.Iterator;
import java.util.Vector;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.queryparsing.functions.AbstractFunction;

/**
 * Implements query parser
 * @author ayadav
 */
public class QueryParser
{
	// query keywords
	public static final String whereKeyword 									= "WHERE";
	public static final String joinKeyword  									= "JOIN";
	
	// registered functions
	//public static final String[] regsitered = {"Overlap", "Distance", "GeojsonOverlap"};

	// equal, less equal, greater equal, less, great, not
    // order is important, equality should come in last.
    // helps in parsing
	public static String [] attributeOperators									= {"<=", ">=", "="};
	
	/*public static final HashMap<String, String> comparisonOpersMap 				= new HashMap<String, String>() 
			{
				{
					comparisonOpersMap.put("<=", "<=");
					comparisonOpersMap.put(">=", ">=");
					comparisonOpersMap.put("=", "=");
				}
			};*/
	
	public static String [] booleanOperators									= {"AND"};
	
	/*public static void queryParsing(String query)
	{
		String [] queryParts = query.split(" ");
	}*/
	
	/*public static Vector<QueryComponent> parseQuery(String query)
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
	}*/
	
	public static Vector<QueryComponent> parseQueryNew(String userQuery)
	{
		//String queryPrefix = "SELECT GUID_TABLE.guid FROM GUID_TABLE ";
		// removing multiple spaces into one
		String after = userQuery.trim().replaceAll(" +", " ");
		String[] spaceParsed = after.split(" ");
		String whereOrJoin = spaceParsed[4].toUpperCase();
		switch(whereOrJoin)
		{
			case whereKeyword:
			{
				return parseWhereQuery(spaceParsed);
				//break;
			}
			case joinKeyword:
			{
				//parseJoinQuery(spaceParsed);
				break;
			}
		}
		return null;
	}
	
	/**
	 * returns a vector of predicates(QueryComponent)
	 * @param spaceParsed
	 * @return
	 */
	private static Vector<QueryComponent> parseWhereQuery( String[] spaceParsed )
	{
		Vector<QueryComponent> queryComponents = new Vector<QueryComponent>();
		
		int startInd 				= 5;
		//int lastStartingIndex 	= 5;
		String predicateString = "";
		while( startInd < spaceParsed.length )
		{
			predicateString = predicateString+spaceParsed[startInd]+" ";
			startInd++;
		}
		String[] ANDParsed = predicateString.split(booleanOperators[0]);
		
		startInd = 0;
		
		while( startInd < ANDParsed.length )
		{
			String curr = ANDParsed[startInd].trim();
			
			//int startInd2 = lastStartingIndex;
			
			//String operOrFunString = ANDParsed[startInd];
			//QueryComponent> toGetFunctionName = new Vector<QueryComponent>();
			QueryComponent currCompo = parsePredicate(curr);
			queryComponents.add(currCompo);
			startInd = startInd + 1;
		}
		return queryComponents;
	}
	
	private static QueryComponent parsePredicate(String predicateString)
	{
		Iterator<String> keyIter = AbstractFunction.registeredFunctionsMap.keySet().iterator();
		boolean isFun = false;
		String functionName = "";
		while( keyIter.hasNext() )
		{
			String funcName = keyIter.next();
			if( predicateString.toUpperCase().startsWith(funcName.toUpperCase()) )
			{
				functionName = funcName;
				//toGetFunctionName.add(funcName);
				isFun = true;
				break;
			}
		}
		QueryComponent currCompo = null;
		if(isFun)
		{
			int startBrack = predicateString.indexOf("(");
			int endBrack = predicateString.indexOf(")");
			
			//String functionName = predicateString.substring(0, startBrack);
			
			String argumentStr = predicateString.substring(startBrack+1, endBrack).trim();
			String[] commaParsed = argumentStr.split(",");
			currCompo = new QueryComponent(QueryComponent.FUNCTION_PREDICATE, 
					functionName, commaParsed);
		}
		else
		{
			// order is imp. first we check for >=, then <= and then =
			for(int i=0;i<attributeOperators.length;i++)
			{
				int ind = predicateString.indexOf(attributeOperators[i]);
				if(ind != -1)
				{
					String attrName = predicateString.substring(0, ind).trim();
					String operator = attributeOperators[i];
					String value = predicateString.substring(ind+operator.length()).trim();
					
					// remove the single and double quotes for String attrs
					
					AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
					String dataType = attrMetaInfo.getDataType();
					
					if( dataType.equals(AttributeTypes.StringType) )
					{
						if(value.length()<=2)
						{
							assert(false);
						}
						
						// removing first and last quotes
						value = value.substring(1, value.length()-1);	
					}
					
					
					currCompo = new QueryComponent(QueryComponent.COMPARISON_PREDICATE, 
							attrName, operator, value);
					break;
				}
			}
		}
		return currCompo;
	}
	
	
	/*private static void parseJoinQuery(String[] spaceParsed)
	{
		Vector<QueryComponent> queryComponents = new Vector<QueryComponent>();
		int startInd 				= 4;
		
		//int lastStartingIndex 	= 5;
		String predicateString = "";
		while( startInd < spaceParsed.length )
		{
			predicateString = predicateString+spaceParsed[startInd]+" ";
			startInd++;
		}
		String[] ANDParsed = predicateString.split(booleanOperators[0]);
		
		startInd = 0;
		
		while( startInd < ANDParsed.length )
		{
			String curr = ANDParsed[startInd].trim();
			
			//int startInd2 = lastStartingIndex;
			
			//String operOrFunString = ANDParsed[startInd];
			//QueryComponent> toGetFunctionName = new Vector<QueryComponent>();
			QueryComponent currCompo = parsePredicate(curr);
			queryComponents.add(currCompo);
		}
		return queryComponents;
	}*/
	
	/*private static QueryComponent getComponent(String predicate)
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
	}*/
	
	
	public static void main(String[] args)
	{
		// query parsing test
		//String query = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE attr1 >= 10 AND attr1 <= 20 AND Overlap(attr1, attr2)";
		String query = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE latitude >= 1 AND longitude <= 140";
		QueryParser.parseQueryNew(query);
	}
}