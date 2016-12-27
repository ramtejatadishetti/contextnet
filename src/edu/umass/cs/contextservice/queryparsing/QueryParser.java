package edu.umass.cs.contextservice.queryparsing;


import java.util.HashMap;
import java.util.Iterator;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;

/**
 * Implements query parser
 * @author ayadav
 */
public class QueryParser
{	
	// equal, less equal, greater equal, less, great, not
    // order is important, equality should come in last.
    // helps in parsing
	public static String [] attributeOperators									= {"<=", ">=", "="};
	
	public static String [] booleanOperators									= {"AND"};
	
	
	/**
	 * Parses the search query and returns the value space. 
	 * @param userQuery
	 * @return
	 */
	public static ValueSpaceInfo parseQuery(String userQuery)
	{
		//String queryPrefix = "SELECT GUID_TABLE.guid FROM GUID_TABLE ";
		// removing multiple spaces into one
		String Query = userQuery.trim().replaceAll(" +", " ");
		//String[] spaceParsed = after.split(" ");
		
		return  parseWhereQuery(Query);
	}
	
	/**
	 * returns a vector of predicates(QueryComponent)
	 * @param spaceParsed
	 * @return
	 */
	private static ValueSpaceInfo parseWhereQuery( String searchQuery )
	{
		ValueSpaceInfo queryValSpace = new ValueSpaceInfo();
		
		HashMap<String, AttributeValueRange> valSpaceBoundary = queryValSpace.getValueSpaceBoundary();
		
		Iterator<String> attrIter = AttributeTypes.attributeMap.keySet().iterator();
		
		while(attrIter.hasNext())
		{
			String attrName = attrIter.next();
			AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
			valSpaceBoundary.put(attrName, 
					new AttributeValueRange(attrMetaInfo.getMinValue(), attrMetaInfo.getMaxValue()));
		}
		
		
		String[] ANDParsed = searchQuery.split(booleanOperators[0]);
		
		int startInd = 0;
		
		while( startInd < ANDParsed.length )
		{
			String curr = ANDParsed[startInd].trim();
			parsePredicate(curr, queryValSpace);
			startInd = startInd + 1;
		}
		return queryValSpace;
	}
	
	private static void parsePredicate(String predicateString, ValueSpaceInfo queryValSpace)
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
				
				addPredicateToValueSpace(attrName, operator, 
						value, queryValSpace);
				break;
			}
		}
	}
	
	private static void addPredicateToValueSpace(String attrName, String operator, 
					String value, ValueSpaceInfo queryValSpace)
	{
		AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
		AttributeValueRange attrValRange = queryValSpace.getValueSpaceBoundary().get(attrName);
		if(attrValRange == null)
		{
			attrValRange = new AttributeValueRange
					(attrMetaInfo.getMinValue(), attrMetaInfo.getMaxValue());
			queryValSpace.getValueSpaceBoundary().put(attrName, attrValRange);
		}
		
		if( operator.equals("<=") )
		{				
			attrValRange.setUpperBound(value);
		}
		else if( operator.equals(">="))
		{
			attrValRange.setLowerBound(value);
		}
		else if(operator.equals("="))
		{
			attrValRange.setLowerBound(value);
			attrValRange.setUpperBound(value);
		}
	}
	
	public static void main(String[] args)
	{
		int NUM_ATTRS = 20;
		// query parsing test
		HashMap<String, AttributeMetaInfo> givenMap = new HashMap<String, AttributeMetaInfo>();
		
		for(int i=0; i < NUM_ATTRS; i++)
		{
			String attrName = "attr"+i;
			AttributeMetaInfo attrInfo =
					new AttributeMetaInfo(attrName, 1+"", 1500+"", AttributeTypes.DoubleType);
			
			givenMap.put(attrInfo.getAttrName(), attrInfo);	
		}
		AttributeTypes.initializeGivenMap(givenMap);
		
		String query = "attr0 >= 100 AND attr5 <= 140";
		ValueSpaceInfo queryValSpace = QueryParser.parseQuery(query);
		
		System.out.println("Query value space "+queryValSpace.toString());
	}
}