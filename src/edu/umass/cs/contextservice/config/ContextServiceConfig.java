package edu.umass.cs.contextservice.config;

/**
 * Context service config file. It contains all configuration
 * parameters.
 * @author adipc
 */
public class ContextServiceConfig
{
	public static enum DatabaseTypes {INMEMORY, MONGODB};
	
	public static enum SchemeTypes {CONTEXTNET, REPLICATE_ALL, QUERY_ALL, MERCURY, HYPERDEX, 
		MERCURYNEW, MERCURYCONSISTENT};
	
	public static final DatabaseTypes DATABASE_TYPE					= DatabaseTypes.INMEMORY;
	
	public static SchemeTypes SCHEME_TYPE							= SchemeTypes.CONTEXTNET;
	
	// attributes configurations
	public static final String CONTEXT_ATTR_PREFIX					= "contextATT";
	// num of attributes
	public static int NUM_ATTRIBUTES								= 100;
	
	public static int NUM_RANGE_PARTITIONS							= 3;
	
	// prints experiment print statements
	public static boolean EXP_PRINT_ON								= false;
	
	// if true, groupGUID is stored, if false it is not stores.
	// false only for experiments, as experiments compare with equation/
	// otherwise should be true.
	public static final boolean  GROUP_INFO_STORAGE					= false;
	
	// if it is true then the group trigger mechanism works, that
	// on each update the GUID is stored and removed from the 
	// affected groups. If false that doesn't happen.
	// false only for experiment comparison with Mercury, Hyperdex,
	// Replicate-All etc.
	public static final boolean GROUP_UPDATE_TRIGGER				= false;
	
	//this flag indicates whether GNS is used or not.
	// In GNSCalls class, it bypasses GNS calls if set to false.
	public static final boolean USE_GNS								= false;
	
	// used in table names in database 
	public static final String MetadataTableSuffix 					= "MetadataTable";
	
	public static final String ValueTableSuffix    					= "ValueInfoTable";
	
	public static final String FullObjectTable    					= "fullObjectTable";
	
	public static final boolean USESQL								= true;
}