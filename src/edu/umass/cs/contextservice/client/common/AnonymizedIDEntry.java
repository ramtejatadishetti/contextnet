package edu.umass.cs.contextservice.client.common;

import java.util.List;

import org.json.JSONArray;

/**
 * The class represents anonymized ID and its 
 * associated attributes(Bu,l) and guids(Gu,l).
 * @author adipc
 *
 */
public class AnonymizedIDEntry 
{	
	private final byte[] anonymizedIDByteArray;
	private final JSONArray attributeSet;
	private final JSONArray guidSet;
	
	public AnonymizedIDEntry(byte[] anonymizedIDByteArray, JSONArray attributeSet, 
			JSONArray guidSet)
	{
		this.anonymizedIDByteArray = anonymizedIDByteArray;
		this.attributeSet = attributeSet;
		this.guidSet = guidSet;
	}
	
	
	public byte[] getID()
	{
		return anonymizedIDByteArray;
	}
	
	public JSONArray getAttributeSet()
	{
		return attributeSet;
	}
	
	public JSONArray getGUIDSet()
	{
		return guidSet;
	}
}