package edu.umass.cs.contextservice.config;

/**
 * Context service config file.
 * It contains all configuration parameters.
 * @author adipc
 */
public class ContextServiceConfig
{
	// properties name, these are read from properties file
	public static final String modelRhoString 						= "modelRho";
	public static final String modelCsByCString 					= "modelCsByC";
	public static final String modelCuByCString 					= "modelCuByC";
	public static final String modelAavgString  					= "modelAavg";
	public static final String triggerEnableString					= "triggerEnable";
	
	
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
	public static String csConfigFileName							= "csConfigFile.txt";
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
	//public static boolean basicSubspaceConfig						= false;
	
	
	
    // on d710 cluster 150 gives the best performance, after that performance remains same.
    // should be at least same as the hyperspace hashing pool size.
    // actually default mysql server max connection is 151. So this should be
    // set in conjuction with that. and also the hyperpsace hashing thread pool
    // size should be set greater than that. These things affect system performance a lot.
	public static final int MYSQL_MAX_CONNECTIONS					= 150;
	
	// it is also important to set this at least the size of the database connection pool.
	public static final int HYPERSPACE_THREAD_POOL_SIZE				= 150;
	
	// mysql result cursor fetches 1 guid at once and stores in memory
	// need this becuase otherwise in large guids case , all the result 
	// is stored in memory by default.
	public static final int MYSQL_CURSOR_FETCH_SIZE					= 1;
	
	// 1000 inserts are batched in one prepared stmt for 
	// inserting subspace partition info
	public static final int SUBSPACE_PARTITION_INSERT_BATCH_SIZE	= 1000;
	
	// this gives minimum of 2^10 subspace partitions if there are 10 
	// attributes and each parititioned twice. 
	public static final int MAXIMUM_NUM_ATTRS_IN_SUBSPACE			= 10;
	
	//  model paramters
	// search/(search+update) ratio
	public static double modelRho										= 0.5;
	// single node search throughput inverse
	public static double modelCsByC            							= 1.0/(69.55366816958512 * 4.0);
	// single node udpate throughput inverse
	public static double modelCuByC            							= 1.0/(153.74028685197356 * 4.0);
	
	public static double modelAavg             							= 4.0;
}