package edu.umass.cs.contextservice.client.anonymizedID;

import java.util.HashMap;
import java.util.List;

import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.gnsclient.client.util.GuidEntry;

/**
 * Doesn't do anything. Returns each guid in the ACL as its anonymized ID.
 * @author adipc
 *
 */
public class NoopAnonymizedIDCreator implements AnonymizedIDCreationInterface
{
	@Override
	public List<AnonymizedIDEntry> computeAnonymizedIDs(GuidEntry myGuidEntry, 
				HashMap<String, List<ACLEntry>> aclList) 
	{
		return null;
	}
}