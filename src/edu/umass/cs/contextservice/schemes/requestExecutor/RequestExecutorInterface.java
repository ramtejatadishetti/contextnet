package edu.umass.cs.contextservice.schemes.requestExecutor;

/**
 * Specifies the request executor interface.
 * All incoming requests are queued and executed when there
 * are some workers free. This is to prevent thrashing in the system, which happens 
 * in privacy case.
 * Each worker also needs additional threads, so we don't want to assign 
 * additional threads to a new request and deprieve an already executing worker
 * from additional thread and cause system thrashing, in which new requests 
 * are taking up all resources and existing request not getting enough resournces/threads
 * so they are also not finishing, new requests also not finishing, which drops system
 * performance drastically.
 * @author adipc
 */
public interface RequestExecutorInterface 
{
	
	
}