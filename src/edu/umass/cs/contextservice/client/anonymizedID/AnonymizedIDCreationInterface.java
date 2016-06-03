package edu.umass.cs.contextservice.client.anonymizedID;

import java.util.HashMap;
import java.util.List;

import org.apache.commons.codec.DecoderException;
import org.json.JSONException;

import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.gnsclient.client.GuidEntry;

/**
 * This interface defines the methods needed to generate 
 * anonymized IDs.
 * Multiple approaches of generating anonymized IDs 
 * implement this interface.
 * @author adipc
 *
 */
public interface AnonymizedIDCreationInterface
{
	public List<AnonymizedIDEntry> 
			computeAnonymizedIDs( GuidEntry myGuidEntry , 
						HashMap<String, List<ACLEntry>> aclList ) throws DecoderException, JSONException;
}