package edu.umass.cs.contextservice.config;

/**
 * Context service config file. It contains all configuration
 * parameters.
 * @author adipc
 */
public class ContextServiceConfig
{
	public static enum DatabaseTypes {INMEMORY, MONGODB};
	
	public static enum SchemeTypes {CONTEXTNET, REPLICATE_ALL, QUERY_ALL, MERCURY, HYPERDEX};
	
	public static final DatabaseTypes DATABASE_TYPE				= DatabaseTypes.INMEMORY;
	
	public static SchemeTypes SCHEME_TYPE							= SchemeTypes.CONTEXTNET;
	
	// attributes configurations
	public static final String CONTEXT_ATTR_PREFIX				= "context";
	// num of attributes
	public static int NUM_ATTRIBUTES								= 100;
	
	// prints experiment print statements
	public static boolean EXP_PRINT_ON							= false;
	
	// if true, groupGUID is stored, if false it is not stores.
	// false only for experiments, as experiments compare with equation/
	// otherwise should be true.
	public static final boolean  GROUP_INFO_STORAGE			= false;
}