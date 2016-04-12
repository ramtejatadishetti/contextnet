package edu.umass.cs.contextservice.client.csprivacytransform;

import edu.umass.cs.contextservice.messages.dataformat.SearchReplyGUIDRepresentationJSON;

/**
 * Represents the search reply transformed message, that contains
 * anonumizedID, which is transformed into realGUID.
 * @author adipc
 *
 */
public class CSSearchReplyTransformedMessage 
{
	private final SearchReplyGUIDRepresentationJSON searchGUIDObj;
	
	public CSSearchReplyTransformedMessage(SearchReplyGUIDRepresentationJSON searchGUIDObj)
	{
		this.searchGUIDObj = searchGUIDObj;
	}

	public SearchReplyGUIDRepresentationJSON getSearchGUIDObj()
	{
		return this.searchGUIDObj;
	}
}