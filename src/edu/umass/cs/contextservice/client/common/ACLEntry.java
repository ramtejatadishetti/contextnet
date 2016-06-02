package edu.umass.cs.contextservice.client.common;

import java.security.PublicKey;

import org.apache.commons.codec.DecoderException;

import edu.umass.cs.contextservice.utils.Utils;

/**
 * Class that is used to represent ACL and also contains a 
 * toJSONObject methods to convert class's object into JSONObject
 * @author adipc
 */
public class ACLEntry
{		
	private final byte[] publicKeyACLMember;
	private final byte[] guidACLMember;
	
	public ACLEntry(byte[] guidACLMember, byte[] publicKeyACLMember)
	{
		this.guidACLMember = guidACLMember;
		this.publicKeyACLMember = publicKeyACLMember;
	}
	
	public ACLEntry(String guidString, PublicKey publicKey) throws DecoderException
	{
		this.guidACLMember = Utils.hexStringToByteArray(guidString);
		this.publicKeyACLMember = publicKey.getEncoded();
	}
	
	
	public byte[] getACLMemberGUID()
	{
		return this.guidACLMember;
	}
	
	public byte[] getPublicKeyACLMember()
	{
		return publicKeyACLMember;
	}
}