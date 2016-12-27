package edu.umass.cs.contextservice.schemes.helperclasses;

import org.json.JSONArray;

/**
 * SearchReplyInfo keeps track of which nodes 
 * a search queries goes to in a subspace.
 * It also keeps track of the reply from a region.
 * @author ayadav
 */
public class SearchReplyInfo 
{
	public int respNodeId;
	public JSONArray replyArray;
	public int numReplies;
}