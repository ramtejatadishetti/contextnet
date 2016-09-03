package edu.umass.cs.contextservice.queryparsing.functions;

import java.awt.geom.Path2D;
import java.awt.geom.Rectangle2D;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.geodesy.GlobalCoordinate;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;

public class GeojsonOverlapFunction extends AbstractFunction
{
	//public static final String latitudeAttrName				= "latitude";
	//public static final String longitudeAttrName			= "longitude";
	
	// java polygon used to repreent geoJSON polygon
	private Path2D geoJSONPolygon 							= null;
	private Vector<GlobalCoordinate> coordVect 				= null;
	private Vector<ProcessingQueryComponent> pqcVector		= null;
	
	// geoJSON can be overlapping with different latitude and longitude
	// attributes. likw workLatitude, workLongitude etc
	private final String latitudeAttrName;
	private final String longitudeAttrName;
	
	
	public GeojsonOverlapFunction( String funcName, String[] funArgs ) throws JSONException
	{
		super(funcName, funArgs);
		//it should be at least 3
		// two for first two attribtues and third for the geoJSON polygon
		assert(funArgs.length >= 3);
		latitudeAttrName  = funArgs[0].trim();
		longitudeAttrName = funArgs[1].trim();
		// reconstruct JSON string, which is parsed by ,
		String jsonString = "";
		for(int i=2;i<funArgs.length;i++)
		{
			if( i < (funArgs.length-1) )
			{
				jsonString = jsonString + funArgs[i]+",";
			}
			else
			{
				jsonString = jsonString + funArgs[i];
			}
		}
		ContextServiceLogger.getLogger().fine("reconstruction of json "+jsonString);
		//this.functionType = functionType;
		JSONObject geoJSON = new JSONObject(jsonString);
		coordVect = getCoordList(geoJSON);
		geoJSONPolygon = new Path2D.Double();
		
		geoJSONPolygon.moveTo( coordVect.get(0).getLatitude(), 
				coordVect.get(0).getLongitude() );
		for(int i = 1; i < coordVect.size(); ++i)
		{
			geoJSONPolygon.lineTo(coordVect.get(i).getLatitude(), 
					coordVect.get(i).getLongitude());
		}
		geoJSONPolygon.closePath();
		
		// adding attributes location attributes
		attrList.add(latitudeAttrName);
		attrList.add(longitudeAttrName);
		
		pqcVector = new Vector<ProcessingQueryComponent>();
		Rectangle2D boundingRect = geoJSONPolygon.getBounds2D();
		ProcessingQueryComponent pqcLat = new ProcessingQueryComponent
				(latitudeAttrName, boundingRect.getMinX()+"", boundingRect.getMaxX()+"");
		
		ProcessingQueryComponent pqcLong = new ProcessingQueryComponent
				(longitudeAttrName, boundingRect.getMinY()+"", boundingRect.getMaxY()+"");
		pqcVector.add(pqcLat);
		pqcVector.add(pqcLong);
		
		ContextServiceLogger.getLogger().fine("GeojsonOverlapFunction bounding rectangle "
				+latitudeAttrName+" "+boundingRect.getMinX()+" "+boundingRect.getMaxX()
				+" "+longitudeAttrName +" "+boundingRect.getMinY()+" "+boundingRect.getMaxY());
	}
	
	private Vector<GlobalCoordinate> getCoordList(JSONObject geoJSONObject)
	{
		Vector<GlobalCoordinate> coordVector = new Vector<GlobalCoordinate>();
		
		try
		{
			JSONArray coordArray = geoJSONObject.getJSONArray("coordinates");
			// based on westy's representation 
			JSONArray newArray = new JSONArray(coordArray.getString(0));
			for(int i=0;i<newArray.length();i++)
			{
				JSONArray coordList = new JSONArray( newArray.getString(i) );
				double longitude = coordList.getDouble(0);
				double latitude = coordList.getDouble(1);
				GlobalCoordinate gc = new GlobalCoordinate(latitude, longitude);
				coordVector.add(gc);
			}
		} catch (JSONException e) 
		{
			e.printStackTrace();
		}
		return coordVector;
	}
	
	@Override
	public Vector<ProcessingQueryComponent> getProcessingQueryComponents() 
	{
		return this.pqcVector;
	}
	
	public static void main(String[] args)
	{
		//JSONArray coordinate = json.getJSONArray("coordinates");
	}

	@Override
	public boolean checkDBRecordAgaistFunction(ResultSet rs) 
	{
		try 
		{
			double latitude = rs.getDouble(latitudeAttrName);
			double longitude = rs.getDouble(longitudeAttrName);
			return geoJSONPolygon.contains(latitude, longitude);
		} catch (SQLException e) 
		{
			e.printStackTrace();
		}
		return false;
	}
}