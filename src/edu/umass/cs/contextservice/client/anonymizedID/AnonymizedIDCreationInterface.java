package edu.umass.cs.contextservice.client.anonymizedID;

import java.util.HashMap;
import java.util.List;

import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;

/**
 * This interface defines the methods needed to generate 
 * anonymized IDs.
 * 
 * Multiple approaches of generating anonymized IDs 
 * implement this interface.
 * @author adipc
 *
 */
public interface AnonymizedIDCreationInterface 
{
	public List<AnonymizedIDEntry> 
					computeAnonymizedIDs(HashMap<String, List<ACLEntry>> aclList);
}