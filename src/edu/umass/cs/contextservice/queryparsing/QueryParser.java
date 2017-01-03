package edu.umass.cs.contextservice.queryparsing;


import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;

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
	 * Parses the search query and returns a hashmap of
	 * attribute and corresponding range pair. 
	 * Only attributes specfied in the query are returned.
	 * @param userQuery
	 * @return
	 */
	public static HashMap<String, AttributeValueRange> parseQuery(String userQuery)
	{
		// removing multiple spaces into one
		String Query = userQuery.trim().replaceAll(" +", " ");
		return  parseWhereQuery(Query);
	}
	
	
	/**
	 * returns a vector of predicates(QueryComponent)
	 * @param spaceParsed
	 * @return
	 */
	private static HashMap<String, AttributeValueRange> parseWhereQuery( String searchQuery )
	{
		HashMap<String, AttributeValueRange> searchQAttrValRange 
								= new HashMap<String, AttributeValueRange>();
		
		String[] ANDParsed = searchQuery.split(booleanOperators[0]);
		
		int startInd = 0;
		
		while( startInd < ANDParsed.length )
		{
			String curr = ANDParsed[startInd].trim();
			parsePredicate(curr, searchQAttrValRange);
			startInd = startInd + 1;
		}
		return searchQAttrValRange;
	}
	
	
	private static void parsePredicate( String predicateString, 
					HashMap<String, AttributeValueRange> searchQAttrValRange )
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
						value, searchQAttrValRange);
				break;
			}
		}
	}
	
	
	private static void addPredicateToValueSpace(String attrName, String operator, 
				String value, HashMap<String, AttributeValueRange> searchQAttrValRange)
	{
		AttributeMetaInfo attrMetaInfo   = AttributeTypes.attributeMap.get(attrName);
		AttributeValueRange attrValRange = searchQAttrValRange.get(attrName);
		
		if(attrValRange == null)
		{
			attrValRange = new AttributeValueRange
					(attrMetaInfo.getMinValue(), attrMetaInfo.getMaxValue());
			searchQAttrValRange.put(attrName, attrValRange);
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
		List<String> attrList = new LinkedList<String>();
		
		for(int i=0; i < NUM_ATTRS; i++)
		{
			String attrName = "attr"+i;
			AttributeMetaInfo attrInfo =
					new AttributeMetaInfo(attrName, 1+"", 1500+"", AttributeTypes.DoubleType);
			
			givenMap.put(attrInfo.getAttrName(), attrInfo);	
			attrList.add(attrName);
		}
		AttributeTypes.initializeGivenMapAndList(givenMap, attrList);
		
		String query = "attr0 >= 100 AND attr5 <= 140";
		HashMap<String, AttributeValueRange> searchQAttrValRange 
											= QueryParser.parseQuery(query);
		
		System.out.println("Query value space "
										+searchQAttrValRange.toString());
	}
}