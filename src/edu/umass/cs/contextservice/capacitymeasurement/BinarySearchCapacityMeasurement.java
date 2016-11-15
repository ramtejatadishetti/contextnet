package edu.umass.cs.contextservice.capacitymeasurement;


/**
 * This class searches for a system capacity using 
 * algorithm similar to binary search.
 * The algorithm starts with 10 requests/s and then movies to
 * 100 requests/s and 1000 requests and so on to find the 
 * lower and upper bounds. After determining lower and
 * upper bounds, the algorithm checks the mean of lower and upper bounds 
 * similar to binary search algorithm until the sending rate and the good put 
 * is within 10%
 * @author ayadav
 */
public class BinarySearchCapacityMeasurement 
{
	// when the sending rate and the goodput is within 10% the search for 
	// capacity stops.
	public static final double STOPPING_THRESHOLD		= 10.0;
	
	//public 
	
	
}