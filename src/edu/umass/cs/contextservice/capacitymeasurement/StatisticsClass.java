package edu.umass.cs.contextservice.capacitymeasurement;

import java.util.HashMap;
import java.util.Vector;

/**
 * 
 * Give an vector of values, this class computes 
 * the statistics mean, median, min, max, 5perc, 95 perc
 * @author adipc
 */
public class StatisticsClass 
{
	public static final String MeanKey 					= "mean";
	public static final String MedianKey 				= "median";
	public static final String MinKey 					= "min";
	public static final String MaxKey 					= "max";
	public static final String FivePercKey 				= "fivePerc";
	public static final String NinetyFivePercKey 		= "ninetyFivePerc";
	
	public static HashMap<String, Double> computeStats(Vector<Double> values)
	{
		if( values.size() <= 0 )
			return null;
		
		values.sort(null);
		
		int minIndex = 0;
		int fivePercIndex = (int)Math.ceil(0.05*values.size()) -1;
		int medianIndex   = (int)Math.ceil(0.5*values.size()) -1;
		int ninetyFivePercIndex = (int)Math.ceil(0.95*values.size()) -1;
		int maxIndex = values.size()-1;
		
		assert(minIndex >= 0);
		assert(fivePercIndex >= 0);
		assert(medianIndex >= 0);
		assert(ninetyFivePercIndex >= 0);
		assert(maxIndex >= 0);
		
		
		double sum = 0.0;
		for (int i=0; i<values.size(); i++)
		{
			sum = sum + values.get(i);
		}
		double mean = sum/values.size();
		double minVal = values.get(minIndex);
		double fivePercValue = values.get(fivePercIndex);
		double medianValue = values.get(medianIndex);
		double ninetyFivePercValue = values.get(ninetyFivePercIndex);
		double maxValue = values.get(maxIndex);
		
		HashMap<String, Double> statMap = new HashMap<String, Double>();
		statMap.put(MeanKey, mean);
		statMap.put(MinKey, minVal);
		statMap.put(FivePercKey, fivePercValue);
		statMap.put(MedianKey, medianValue);
		statMap.put(NinetyFivePercKey, ninetyFivePercValue);
		statMap.put(MaxKey, maxValue);
		return statMap;
	}
	
	public static String toString(HashMap<String, Double> statMap)
	{
		String str ="";
		str = MeanKey+"="+statMap.get(MeanKey)+" , "+MinKey+"="+statMap.get(MinKey)+" , "
				+FivePercKey+"="+statMap.get(FivePercKey)+" , "+MedianKey+"="+statMap.get(MedianKey)+" , "
				+NinetyFivePercKey+"="+statMap.get(NinetyFivePercKey)+" , "+MaxKey+"="+statMap.get(MaxKey);
		return str;
	}
}