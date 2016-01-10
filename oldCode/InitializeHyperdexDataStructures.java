package edu.umass.cs.contextservice.utils;

import java.util.HashMap;
import java.util.Map;

import org.hyperdex.client.Client;
import org.hyperdex.client.HyperDexClientException;

import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.schemes.old.HyperdexSchemeNew;

public class InitializeHyperdexDataStructures
{
	public static final double rangePartitionSize				= 100.0;
	
	
	public static void main(String[] args)
	{
		int numAttrs = Integer.parseInt(args[0]);
		
		Client hyperdexClient = new Client(HyperdexSchemeNew.HYPERDEX_IP_ADDRESS[0], HyperdexSchemeNew.HYPERDEX_PORT[0]);
		
		for(int i=0; i<numAttrs; i++)
		{
			String attrName = ContextServiceConfig.CONTEXT_ATTR_PREFIX+i;
			String keySpaceName = attrName+"Keyspace";
			double start = AttributeTypes.MIN_VALUE;
			
			ContextServiceLogger.getLogger().fine("Initializing attribute "+i);
			
			while(start <= AttributeTypes.MAX_VALUE )
			{
				double end = Math.min(start+rangePartitionSize, AttributeTypes.MAX_VALUE);
				String rangeKey = attrName+":"+start+":"+end;
				ContextServiceLogger.getLogger().fine("rangeKey "+rangeKey);
				
				Map<String, Object> rangeAttrs = new HashMap<String, Object>();
				rangeAttrs.put(HyperdexSchemeNew.LOWER_RANGE_ATTRNAME, start);
				rangeAttrs.put(HyperdexSchemeNew.UPPER_RANGE_ATTRNAME, end);
				
				try 
				{
					hyperdexClient.put(keySpaceName, rangeKey, rangeAttrs);
					ContextServiceLogger.getLogger().fine("Entry put in "+keySpaceName);
					// also put an entry in 
					
					java.util.Map<String, String> activeQueryMap = new java.util.HashMap<String, String>();
					
					Map<String, Object> attrs = new HashMap<String, Object>();
					attrs.put(HyperdexSchemeNew.ACTIVE_QUERY_MAP_NAME, activeQueryMap);
					
					hyperdexClient.put(HyperdexSchemeNew.RANGE_KEYSPACE, rangeKey, attrs);
					ContextServiceLogger.getLogger().fine("Entry put in "+HyperdexSchemeNew.RANGE_KEYSPACE);
				} catch (HyperDexClientException e) 
				{
					e.printStackTrace();
				}
				
				start = end+1;
			}
			ContextServiceLogger.getLogger().fine("\n\n");
		}
	}
}