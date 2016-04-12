package edu.umass.cs.contextservice.client.anonymizedID;

import java.util.HashMap;
import java.util.List;

import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;

/**
 * Doesn't do anything. Returns each guid in the ACL as its anonymized ID.
 * @author adipc
 *
 */
public class NoopAnonymizedIDCreator implements AnonymizedIDCreationInterface
{
	@Override
	public List<AnonymizedIDEntry> computeAnonymizedIDs(HashMap<String, List<ACLEntry>> aclList) 
	{
		return null;
	}
}