package edu.umass.cs.contextservice.processing;

import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.database.records.MetadataTableInfo;

/**
 * Stores the components of the query
 * @author ayadav
 */
public class QueryComponent
{
	// macros as JSON Keys
	public static final String AttrName					= "ATTR_NAME";
	public static final String LeftOper					= "LEFT_OPER";
	public static final String LeftValue				= "LEFT_VAL";
	public static final String RightOper				= "RIGHT_OPER";
	public static final String RightVal					= "RIGHT_VAL";
	public static final String CompId					= "COMP_ID";
	public static final String NumRepForComp			= "NUM_REP_COMP";

	
	// represents  leftValue leftOperator attributeName rightOperator rightValue, like 10 <= a <= 20
	private final String attributeName;
	private final String leftOperator;
	private final double leftValue;
	private final String rightOperator;
	private final double rightValue;
	
	// A query is split into many components, each component within
	// a query has a unique ID.
	private int componentID;
	
	// indicates the number of replies received for this component
	private int numCompReplyRecvd = 0;
	
	// indicates the total number of replies received for this component
	private int totalCompReply;
	
	private List<MetadataTableInfo<Integer>> qcMetadataInfo;
	
	public QueryComponent( String attributeName, String leftOperator, double leftValue, 
			String rightOperator, double rightValue )
	{
		this.attributeName = attributeName;
		this.leftOperator = leftOperator;
		this.leftValue = leftValue;
		this.rightOperator = rightOperator;
		this.rightValue = rightValue;
	}
	
	public String getAttributeName()
	{
		return attributeName;
	}
	
	public double getLeftValue()
	{
		return leftValue;
	}
	
	public double getRightValue()
	{
		return rightValue;
	}
	
	public void setComponentID(int componentID)
	{
		this.componentID = componentID;
	}
	
	public int getComponentID()
	{
		return this.componentID;
	}
	
	public void setTotalCompReply(int totalCompReply)
	{
		this.totalCompReply = totalCompReply;
	}
	
	public synchronized void updateNumCompReplyRecvd()
	{
		this.numCompReplyRecvd++;
	}
	
	public int getTotalCompReply()
	{
		return totalCompReply;
	}
	
	public int getNumCompReplyRecvd()
	{
		return numCompReplyRecvd;
	}
	
	public void setValueNodeArray(List<MetadataTableInfo<Integer>> qcMetadataInfo)
	{
		this.qcMetadataInfo = qcMetadataInfo;
	}
	
	public JSONObject getJSONObject() throws JSONException
	{
		JSONObject qcJSON = new JSONObject();
		qcJSON.put(QueryComponent.AttrName, this.attributeName);
		qcJSON.put(QueryComponent.LeftOper, this.leftOperator);
		qcJSON.put(QueryComponent.LeftValue, this.leftValue);
		qcJSON.put(QueryComponent.RightOper, this.rightOperator);
		qcJSON.put(QueryComponent.RightVal, this.rightValue);
		qcJSON.put(QueryComponent.CompId, this.componentID);
		return qcJSON;
	}
	
	public static QueryComponent getQueryComponent(JSONObject jobj) throws JSONException
	{
		QueryComponent qc = new QueryComponent(jobj.getString(QueryComponent.AttrName), 
				jobj.getString(QueryComponent.LeftOper), jobj.getDouble(QueryComponent.LeftValue), 
				jobj.getString(QueryComponent.RightOper), jobj.getDouble(QueryComponent.RightVal));
		qc.setComponentID(jobj.getInt(QueryComponent.CompId));
		
		return qc;
	}
	
	public String toString()
	{
		return this.leftValue + " " + this.leftOperator
				+ " " + this.attributeName + " " + this.rightOperator + " " + this.rightValue;
	}
}