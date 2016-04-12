package edu.umass.cs.contextservice.client.common;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.utils.Utils;

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
		assert(anonymizedIDByteArray != null);
		assert(attributeSet != null);
		assert(guidSet != null);
		assert(attributeSet.length()>0);
		assert(guidSet.length()>0);
		
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
	
	public String toString()
	{
		String str = "ID: "+Utils.bytArrayToHex(anonymizedIDByteArray)+" AttrSet: "+attributeSet+" GuidSet: ";
		for(int i=0;i<guidSet.length();i++)
		{
			byte[] guidBytes;
			try 
			{
				guidBytes = (byte[]) guidSet.get(i);
				str=str+Utils.bytArrayToHex(guidBytes)+",";
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
		return str;
	}
}