package edu.umass.cs.contextservice.schemes.helperclasses;

import java.util.HashMap;

public class SubspaceSearchReplyInfo 
{
	public int subspaceId;
	// key is node Id here.
	public HashMap<Integer, RegionInfoClass> overlappingRegionsMap;
	public int regionRepliesCounter = 0;
}