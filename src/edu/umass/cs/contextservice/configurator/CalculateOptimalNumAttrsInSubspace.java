package edu.umass.cs.contextservice.configurator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;

/**
 * Calculates the optimal number of attributes in a subspace
 * by minimizing the throughput equation in the model.
 * Calls the python script internally
 * 
 * @author adipc
 */
public class CalculateOptimalNumAttrsInSubspace 
{
	private static final String basicResultLine 	 = "BASIC OPTIMIZATION RESULT H";
	private static final String replicatedResultLine = "REPLICATED OPTIMIZATION RESULT H";
	
	
	private double numNodes;
	private double numAttrs;
	private double optimalH;
	private double funcVal;
	private double nonOptVal;
	// true if basic configuration is used.
	// false if replicated configuration is used.
	private boolean basicOrReplicated;
	
	public CalculateOptimalNumAttrsInSubspace(double numNodes, double numAttrs)
	{
		this.numNodes = numNodes;
		this.numAttrs = numAttrs;
		try
		{
			executeOptimizerScript();
		}
		catch(IOException ex)
		{
			ex.printStackTrace();
			assert(false);
		}
	}
	
	public int getOptimalH()
	{
		return (int) optimalH;
	}
	
	public double getOptimalDenominatorFunValue()
	{
		return funcVal;
	}
	
	public double getNonOptDenominatorFunValue()
	{
		return nonOptVal;
	}
	
	public boolean getBasicOrReplicated()
	{
		return this.basicOrReplicated;
	}
	
	private void executeOptimizerScript() throws IOException
	{
		Process p = Runtime.getRuntime().exec("python conf/optimizerScript/HOptimizerJavaCallable.py "
	+ContextServiceConfig.modelRho+" "+numNodes+" "+ContextServiceConfig.modelCsByC
	+" "+ContextServiceConfig.modelCuByC+" "+numAttrs+" "+ContextServiceConfig.modelAavg);
		
		BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String currline = in.readLine();
		//double optimalH = -1;
		funcVal = -1;
		nonOptVal  = -1;
		while(currline != null)
		{
			//System.out.println(currline);
			if( currline.contains(basicResultLine) )
			{
				//ContextServiceConfig.basicSubspaceConfig = true;
				basicOrReplicated = true;
				String[] parsed = currline.split(" ");
				optimalH = Double.parseDouble(parsed[4]);
				funcVal = Double.parseDouble(parsed[6]);
				nonOptVal = Double.parseDouble(parsed[8]);
				ContextServiceLogger.getLogger().fine("BASIC CONFIG optimalH "+optimalH
						+" funcVal "+funcVal+" nonOptVal "+nonOptVal);
				break;
			}
			else if(currline.contains(replicatedResultLine) )
			{
				//ContextServiceConfig.basicSubspaceConfig = false;
				basicOrReplicated = false;
				String[] parsed = currline.split(" ");
				optimalH = Double.parseDouble(parsed[4]);
				funcVal = Double.parseDouble(parsed[6]);
				nonOptVal = Double.parseDouble(parsed[8]);
				ContextServiceLogger.getLogger().fine("REPLICATED CONFIG optimalH "+optimalH
						+" funcVal "+funcVal+" nonOptVal "+nonOptVal);
				break;
			}
			currline = in.readLine();
		}
	}
}