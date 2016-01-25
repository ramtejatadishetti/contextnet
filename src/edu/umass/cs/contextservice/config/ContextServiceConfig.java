package edu.umass.cs.contextservice.config;

/**
 * Context service config file. It contains all configuration
 * parameters.
 * @author adipc
 */
public class ContextServiceConfig
{
	// path where all config files are stored, like node setup, attribute info, subspace info
	public static String configFileDirectory;
	
	public static enum SchemeTypes {HYPERSPACE_HASHING};
	
	public static SchemeTypes SCHEME_TYPE							= SchemeTypes.HYPERSPACE_HASHING;
	
	// prints experiment print statements
	public static boolean EXP_PRINT_ON								= false;
	
	// if true, groupGUID is stored, if false it is not stores.
	// false only for experiments, as experiments compare with equation/
	// otherwise should be true.
	public static final boolean  GROUP_INFO_COMPONENT				= false;
	
	// if it is true then the group trigger mechanism works, that
	// on each update the GUID is stored and removed from the 
	// affected groups. If false that doesn't happen.
	// false only for experiment comparison with Mercury, Hyperdex,
	// Replicate-All etc.
	//public static final boolean GROUP_UPDATE_TRIGGER				= false;
	
	//this flag indicates whether GNS is used or not.
	// In GNSCalls class, it bypasses GNS calls if set to false.
	public static final boolean USE_GNS								= false;
	
	// used in table names in database 
	public static final String MetadataTableSuffix 					= "MetadataTable";
	
	public static final String ValueTableSuffix    					= "ValueInfoTable";
	
	public static final String FullObjectTable    					= "fullObjectTable";
	
	public static final String groupGUIDTable						= "groupGUIDTable";
	
	public static final boolean USESQL								= true;
	
	public static boolean DELAY_MEASURE_PRINTS						= true;
	
	public static boolean DELAY_PROFILER_ON							= true;
	
	public static boolean DATABASE_SIZE_PRINT						= true;
	
	// config files
	public static String subspaceInfoFileName						= "subspaceInfo.txt";
	public static String attributeInfoFileName						= "attributeInfo.txt";
	public static String nodeSetupFileName							= "contextServiceNodeSetup.txt";
	public static String dbConfigFileName							= "dbNodeSetup.txt";
	
	//control if full guids are sent in the search query
	// reply, if false only sends the number of guids, not
	// the actual guids
	public static final boolean sendFullReplies						= true;
	
	//if false, replies for any update messages will not be sent
	// just for measuring update throughout and time in experiments
	//public static final boolean sendUpdateReplies					= true;
	
	// if true group update trigger is enabled, not enabled if false
	public static final boolean TRIGGER_ENABLED						= false;
	
	//public static final int startNodeID							= 0;
	
	//public static enum DatabaseTypes {INMEMORY, MONGODB};
	
	//public static enum SchemeTypes {CONTEXTNET, REPLICATE_ALL, QUERY_ALL, MERCURY, HYPERDEX, 
	//	MERCURYNEW, MERCURYCONSISTENT};
	//public static final DatabaseTypes DATABASE_TYPE				= DatabaseTypes.INMEMORY;
	
	// attributes configurations
	//public static final String CONTEXT_ATTR_PREFIX				= "contextATT";
	// num of attributes
	//public static int NUM_ATTRIBUTES								= 100;
	//public static int NUM_RANGE_PARTITIONS						= 3;
}