package edu.umass.cs.contextservice.client.anonymizedID;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.client.SecureContextClientInterface;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.utils.Utils;


public class SubspaceBasedAnonymizedIDCreator implements AnonymizedIDCreationInterface
{
	private final HashMap<Integer, JSONArray> subspaceAttrMap;
	private final Random anonymizedIDRand					= new Random();
	
	public SubspaceBasedAnonymizedIDCreator(HashMap<Integer, JSONArray> subspaceAttrMap)
	{
		this.subspaceAttrMap = subspaceAttrMap;
	}
	
	@Override
	public List<AnonymizedIDEntry> computeAnonymizedIDs
	(HashMap<String, List<ACLEntry>> aclMap) 
	{
		try
		{
			// each element is AnonymizedIDStoringClass
			List<AnonymizedIDEntry> anonymizedIDList 
								= new LinkedList<AnonymizedIDEntry>();
			ContextServiceLogger.getLogger().fine("Size of attrACLMap "+aclMap.size() );
			
			Iterator<Integer> subspaceAttrIter 
										= this.subspaceAttrMap.keySet().iterator();
			
			while( subspaceAttrIter.hasNext() )
			{
				int mapKey = subspaceAttrIter.next();
				
				JSONArray attrArray = this.subspaceAttrMap.get(mapKey);
				
				// String is GUIDString
				HashMap<String, List<String>> guidToAttributesMap 
								= new HashMap<String, List<String>>();
				
				for( int i=0; i<attrArray.length(); i++ )
				{
					String currAttr = attrArray.getString(i);
					
					ContextServiceLogger.getLogger().fine(" mapKey "+mapKey+" currAttr "+currAttr);
					
					List<ACLEntry> attrACL = aclMap.get(currAttr);
					//List<byte[]> publicKeyACLMembers = attrACL.getPublicKeyACLMembers();
					
					for( int j=0; j<attrACL.size(); j++ )
					{
						byte[] publicKeyByteArray = (byte[]) attrACL.get(j).getPublicKeyACLMember();
						byte[] guidByteArray = Utils.convertPublicKeyToGUIDByteArray(publicKeyByteArray);
						String guidString = Utils.bytArrayToHex(guidByteArray);
								
						ContextServiceLogger.getLogger().fine
						("mapKey "+mapKey+" currAttr "+currAttr+" guid "+Utils.bytArrayToHex(guidByteArray));
						
						List<String> attrListBuk = guidToAttributesMap.get(guidString);
						
						if( attrListBuk == null )
						{
							attrListBuk = new LinkedList<String>();
							attrListBuk.add(currAttr);
							guidToAttributesMap.put(guidString, attrListBuk);
						}
						else
						{
							attrListBuk.add(currAttr);
						}
					}
				}
				
				ContextServiceLogger.getLogger().fine("Size of guidToAttributesMap "
						+guidToAttributesMap.size());
				
				
				// map computed now compute anonymized IDs
				// we sort the list of attributes, so that different permutations of same set 
				// becomes same and then just add them to hashmap for finding distinct sets.
				
				// JSONArray of attributes is the String.
				// JSONArray cannot be directly used.
				HashMap<String, JSONArray> attributesToGuidsMap 
											= new HashMap<String, JSONArray>();
				
				Iterator<String> guidIter = guidToAttributesMap.keySet().iterator();
				
				while( guidIter.hasNext() )
				{
					String currGUIDString = guidIter.next();
					byte[] currGUID = Utils.hexStringToByteArray(currGUIDString);
					ContextServiceLogger.getLogger().fine("mapKey "+mapKey+" printing guidToAttributesMap keys "
							+currGUIDString);
					
					List<String> attrListBuk = guidToAttributesMap.get(currGUIDString);
					// sorting by natural String order
					attrListBuk.sort(null);
					
					// it is just concantenation of attrs in sorted order
					JSONArray attrArrayBul = new JSONArray();
					for( int i=0; i < attrListBuk.size(); i++)
					{
						attrArrayBul.put(attrListBuk.get(i));
					}
					
					ContextServiceLogger.getLogger().fine("mapKey "+mapKey+" printing attrArrayBul"
							+attrArrayBul.toString());
					
					JSONArray guidsListArray = attributesToGuidsMap.get(attrArrayBul.toString());
					
					if( guidsListArray == null )
					{
						guidsListArray = new JSONArray();
						guidsListArray.put(currGUID);
						attributesToGuidsMap.put(attrArrayBul.toString(), guidsListArray);
					}
					else
					{
						guidsListArray.put(currGUID);
					}
				}
				
				ContextServiceLogger.getLogger().fine("Size of attributesToGuidsMap "
																	+attributesToGuidsMap.size());
				// now assign anonymized ID
				
				//HashMap<String, List<byte[]>> attributesToGuidsMap 
				//	= new HashMap<String, List<byte[]>>();

				Iterator<String> attrSetIter = attributesToGuidsMap.keySet().iterator();
				
				while( attrSetIter.hasNext() )
				{
					JSONArray attrSet = new JSONArray(attrSetIter.next());
					JSONArray guidSet = attributesToGuidsMap.get(attrSet);
					
					byte[] anonymizedID 
								= new byte[SecureContextClientInterface.SIZE_OF_ANONYMIZED_ID];
					
					anonymizedIDRand.nextBytes(anonymizedID);
					
					AnonymizedIDEntry anonymizedIDObj 
						= new AnonymizedIDEntry(anonymizedID, attrSet, guidSet);
					anonymizedIDList.add(anonymizedIDObj);
				}
			}	
			return anonymizedIDList;
		}
		catch( JSONException jsoEx )
		{
			jsoEx.printStackTrace();
		}
		return null;
	}
	
}