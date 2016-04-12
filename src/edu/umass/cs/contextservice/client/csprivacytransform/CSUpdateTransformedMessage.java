package edu.umass.cs.contextservice.client.csprivacytransform;

import java.util.HashMap;
import java.util.Iterator;

import edu.umass.cs.contextservice.messages.dataformat.AttrValueRepresentationJSON;
import edu.umass.cs.contextservice.utils.Utils;

/**
 * This message stores  the anonymized ID, attrValuePairs,  
 * the mapping information to real GUID for each attribute 
 * associated to this anonymized ID.
 * @author adipc
 *
 */
public class CSUpdateTransformedMessage 
{
	private final byte[] anonymizedID; 
	
	HashMap<String, AttrValueRepresentationJSON> attrValMap;
	
	public CSUpdateTransformedMessage(byte[] anonymizedID, 
			HashMap<String, AttrValueRepresentationJSON> attrValMap)
	{
		this.anonymizedID = anonymizedID;
		this.attrValMap = attrValMap;
	}
	
	public byte[] getAnonymizedID()
	{
		return this.anonymizedID;
	}
	
	public HashMap<String, AttrValueRepresentationJSON> getAttrValMap()
	{
		return this.attrValMap;
	}
	
	public String toString()
	{
		String str="anonymized ID "+Utils.bytArrayToHex(anonymizedID);
		Iterator<String> attrIter = attrValMap.keySet().iterator();
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			AttrValueRepresentationJSON attrVal = attrValMap.get(attrName);
			str=str+" attrName "+attrName+" val rep "+attrVal.toString();
		}
		return str;
	}
}