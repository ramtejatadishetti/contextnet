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
	public static final String modelCtByCString						= "modelCtByC";
	public static final String modelCiByCString						= "modelCiByC";
	public static final String modelCminByCString					= "modelCminByC";
	
	public static final String modelSearchResString					= "modelSearchRes";
	
	public static final String disableOptimizerString				= "disableOptimizer";
	public static final String basicConfigString					= "basicConfig";
	public static final String optimalHString						= "optimalH";
	public static final String privacyEnabledString					= "privacyEnabled";
	
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
	
	public static final boolean PROFILER_THREAD						= false; 
	
	// config files
	public static String csConfigFileName							= "csConfigFile.txt";
	public static String subspaceInfoFileName						= "subspaceInfo.txt";
	public static String attributeInfoFileName						= "attributeInfo.txt";
	public static String nodeSetupFileName							= "contextServiceNodeSetup.txt";
	public static String dbConfigFileName							= "dbNodeSetup.txt";
	
	//control if full guids are sent in the search query
	// reply, if false only sends the number of guids, not
	// the actual guids
	public static  boolean sendFullReplies							= true;
	
	// if this is set to true, then mysql table selects
	// return results row by row. If set to false then
	// default mysql select semantics is used which fetches all
	// results in memory on a select, but on large result sizes can cause
	// memory overflow.
	public static final boolean rowByRowFetchingEnabled				= false;
	
	//if false, replies for any update messages will not be sent
	// just for measuring update throughout and time in experiments
	//public static final boolean sendUpdateReplies					= true;
	
	// if true group update trigger is enabled, not enabled if false
	public static boolean TRIGGER_ENABLED							= false;
	
	
	// if set to true then there is a primary node for each groupGUID
	// and search always gores through that and doesn't update trigger info if its is repeated.
	public static boolean UniqueGroupGUIDEnabled					= false;
	
	// disables most of the mysql db operations, apart from the reading of 
	// subspace info. Just for throuhgput testing.
	public static boolean disableMySQLDB							= false;
	
	// if set to true basic subspace config is enabled.
	//public static boolean basicSubspaceConfig						= false;
	
    // on d710 cluster 150 gives the best performance, after that performance remains same.
    // should be at least same as the hyperspace hashing pool size.
    // actually default mysql server max connection is 151. So this should be
    // set in conjuction with that. and also the hyperpsace hashing thread pool
    // size should be set greater than that. These things affect system performance a lot.
	// change back to 214 for experiments.
	public static final int MYSQL_MAX_CONNECTIONS					= 214;
	
	// it is also important to set this at least the size of the database connection pool.
	public static final int HYPERSPACE_THREAD_POOL_SIZE				= 214;
	
	//public static final int PRIVACY_THREAD_POOL_SIZE				= 214;
	
	//public static final int HYPERSPACEDB_THREAD_POOL_SIZE			= 214;
	
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
	
	
	public static boolean PRIVACY_ENABLED							= false;
	
	
	// if this is set to true then the context service client will do decryptions 
	// on receiving the search reply.
	public static boolean DECRYPTIONS_ON_SEARCH_REPLY_ENABLED		= true;
	
	// if true, no encryption happens. cipher text and plain text are same.
	public static boolean NO_ENCRYPTION								= false;
	
	// if set to true then anonymized IDs will have random attr-val pairs.
	// only for testing.
	public static boolean RAND_VAL_JSON								= false;
	
	
	public static final String AssymmetricEncAlgorithm				= "RSA";
	public static final String SymmetricEncAlgorithm				= "DES";
	public static final String STRING_ENCODING						= "UTF-8";
	
	// if true some debugging information will be computed and printed.
	public static final boolean DEBUG_MODE							= false;
	
	
	//  model parameters
	// search/(search+update) ratio
	public static double modelRho									= 0.5;
	// single node search throughput inverse
	public static double modelCsByC            						= 0.005319149;
	// single node udpate throughput inverse
	public static double modelCuByC            						= 0.001388889;
	
	public static double modelAavg             						= 4.0;
	
	public static double modelCtByC             					= 0.000009028;
	
	public static double modelCiByC             					= 0.004127837;
	
	public static double modelCminByC								= 0.00063231;
	
	// search query automiatically expires after this time
	public static long modelSearchRes								= 30000;
	
	// if set to true then optimizer is disabled
	public static boolean disableOptimizer							= false;
	
	// basic config is used if set to true.
	// only used if optimizer is disabled, otherwise the optimizer given config will be used.
	public static boolean basicConfig								= true;
	
	// can be set from config files, if the optimizer is disabled. Otherwise will come from
	// optimizer. If false replicated config will be used and optimizer disabled
	public static double optimalH									= 2.0;
	
	// security things
	public static final int KEY_LENGTH_SIZE							= 1024;
	
	
	// current encryption generated 128 bytes, if that changes then this has to change.
	public static final int REAL_ID_ENCRYPTION_SIZE					= 128;
	
	
	// maximum length of an attribute name, used in varchar mysql table
	public static final int MAXIMUM_ATTRNAME_LENGTH					= 100;
	
	// just for debugging different components separately.
	//public static final boolean DISABLE_SECONDARY_SUBSPACES_UPDATES			= false;
	//public static final boolean DISABLE_PRIMARY_SUBSPACE_UPDATES				= false;
}