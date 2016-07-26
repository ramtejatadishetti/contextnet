package edu.umass.cs.contextservice.database.triggers;

import org.json.JSONException;
import org.json.JSONObject;

public class GroupGUIDInfoClass
{
	private enum Keys {GROUP_GUID, USER_IP, USER_PORT};
	
	//update requestID of the update mesg
	private final String groupGUID;
	
	// subspace num
	private final String userIP;
	
	private final int userPort;
	
	/*
	 * sourceID will be the ID of the node that 
	 * recvd query from the user.
	 */
	public GroupGUIDInfoClass( String groupGUID, String userIP, int userPort )
	{
		this.groupGUID = groupGUID;
		this.userIP = userIP;
		this.userPort = userPort;
	}
	
	public GroupGUIDInfoClass( JSONObject json ) throws JSONException
	{
		this.groupGUID = json.getString(Keys.GROUP_GUID.toString());
		this.userIP = json.getString(Keys.USER_IP.toString());
		this.userPort = json.getInt(Keys.USER_PORT.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = new JSONObject();
		json.put(Keys.GROUP_GUID.toString(), groupGUID);
		json.put(Keys.USER_IP.toString(), this.userIP);
		json.put(Keys.USER_PORT.toString(), this.userPort);
		return json;
	}
	
	public String getGroupGUID()
	{
		return groupGUID;
	}
	
	public String getUserIP()
	{
		return this.userIP;
	}
	
	public int getUserPort()
	{
		return this.userPort;
	}
	
	public static void main(String[] args)
	{
	}
}