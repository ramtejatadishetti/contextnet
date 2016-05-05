package edu.umass.cs.contextservice.test;

import java.util.HashMap;
import java.util.List;

import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.gnsclient.client.GuidEntry;

/**
 * Represents the user entry.
 * User keys, acl info, anonymized IDs 
 * needed to be stored for privacy experiment.
 * @author adipc
 */
public class UserEntry 
{
	private final GuidEntry myGuidEntry;
	private HashMap<String, List<ACLEntry>> aclMap;
	private List<AnonymizedIDEntry> anonymizedIDList;
	// denotes the union of ACLs,
	// first a group of guids are chosen then they 
	// are distributed into
	// ACLs.
	private HashMap<String, ACLEntry> unionOfACLsMap;
	
	// acl classes, where key is the class num.
	private HashMap<Integer, List<ACLEntry>> aclClasses;
	
	public UserEntry(GuidEntry myGuidEntry)
	{
		this.myGuidEntry = myGuidEntry;
	}
	
	public GuidEntry getGuidEntry()
	{
		return this.myGuidEntry;
	}
	
	public HashMap<String, List<ACLEntry>> getACLMap()
	{
		return this.aclMap;
	}

	public List<AnonymizedIDEntry> getAnonymizedIDList()
	{
		return this.anonymizedIDList;
	}
	
	public void setACLMap(HashMap<String, List<ACLEntry>> aclMap)
	{
		this.aclMap = aclMap;
	}
	
	public void setAnonymizedIDList(List<AnonymizedIDEntry> 
												anonymizedIDList)
	{
		this.anonymizedIDList = anonymizedIDList;
	}
	
	public HashMap<String, ACLEntry> getUnionOfACLs()
	{
		return this.unionOfACLsMap;
	}
	
	public void setUnionOfACLs(HashMap<String, ACLEntry> unionOfACLs)
	{
		this.unionOfACLsMap = unionOfACLs;
	}
	
	public void setACLClasses(HashMap<Integer, List<ACLEntry>> aclClasses)
	{
		this.aclClasses = aclClasses;
	}
}