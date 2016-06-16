package edu.umass.cs.contextservice.networktransmission.packets;


/**
 * This class defines all the packet types in 
 * context service.
 * @author adipc
 */
public class PacketTypes 
{
	public static final int QUERY_MSG_FROM_USER 								= 1;
	public static final int VALUE_UPDATE_MSG_FROM_GNS							= 2; 
	public static final int QUERY_MSG_FROM_USER_REPLY							= 3;
	public static final int VALUE_UPDATE_MSG_FROM_GNS_REPLY						= 4;
	public static final int REFRESH_TRIGGER										= 5;
	public static final int QUERY_MESG_TO_SUBSPACE_REGION						= 6;
	public static final int QUERY_MESG_TO_SUBSPACE_REGION_REPLY					= 7;
	public static final int VALUEUPDATE_TO_SUBSPACE_REGION_MESSAGE				= 8;
	public static final int GET_MESSAGE											= 9;
	public static final int GET_REPLY_MESSAGE									= 10;
	public static final int VALUEUPDATE_TO_SUBSPACE_REGION_REPLY_MESSAGE		= 11;
	public static final int QUERY_TRIGGER_MESSAGE								= 12;
	public static final int UPDATE_TRIGGER_MESSAGE								= 13;
	public static final int UPDATE_TRIGGER_REPLY_MESSAGE						= 14;
	public static final int CONFIG_REQUEST										= 15;
	public static final int CONFIG_REPLY										= 16;
	public static final int ACLUPDATE_TO_SUBSPACE_REGION_MESSAGE				= 17;
	public static final int ACLUPDATE_TO_SUBSPACE_REGION_REPLY_MESSAGE			= 18;
}