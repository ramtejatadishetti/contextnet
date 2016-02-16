package edu.umass.cs.contextservice.config;

/**
 * Context service config file.
 * It contains all configuration parameters.
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
	
	public static boolean DELAY_MEASURE_PRINTS						= false;
	
	public static boolean DELAY_PROFILER_ON							= false;
	
	public static boolean DATABASE_SIZE_PRINT						= false;
	
	// config files
	public static String subspaceInfoFileName						= "subspaceInfo.txt";
	public static String attributeInfoFileName						= "attributeInfo.txt";
	public static String nodeSetupFileName							= "contextServiceNodeSetup.txt";
	public static String dbConfigFileName							= "dbNodeSetup.txt";
	
	//control if full guids are sent in the search query
	// reply, if false only sends the number of guids, not
	// the actual guids
	public static final boolean sendFullReplies						= false;
	
	//if false, replies for any update messages will not be sent
	// just for measuring update throughout and time in experiments
	//public static final boolean sendUpdateReplies					= true;
	
	// if true group update trigger is enabled, not enabled if false
	public static boolean TRIGGER_ENABLED							= false;
	
	// if set to true basic subspace config is enabled.
	public static boolean basicSubspaceConfig						= false;
}