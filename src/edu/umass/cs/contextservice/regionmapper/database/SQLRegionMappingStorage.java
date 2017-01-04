package edu.umass.cs.contextservice.regionmapper.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.config.ContextServiceConfig.SQL_DB_TYPE;
import edu.umass.cs.contextservice.database.datasource.AbstractDataSource;
import edu.umass.cs.contextservice.database.datasource.AbstractDataSource.DB_REQUEST_TYPE;
import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;
import edu.umass.cs.contextservice.regionmapper.helper.RegionInfo;

public class SQLRegionMappingStorage extends AbstractRegionMappingStorage
{	
	private final AbstractDataSource dataSource;
	private final HashMap<String, AttributeMetaInfo> attributeMap;
	
	public SQLRegionMappingStorage( AbstractDataSource dataSource, 
			HashMap<String, AttributeMetaInfo> attributeMap )
	{
		this.dataSource = dataSource;
		this.attributeMap = attributeMap;
	}
	
	
	@Override
	public void createTables()
	{
		Connection myConn  = null;
		Statement  stmt    = null;
		
		try
		{
			myConn = dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
			stmt   = myConn.createStatement();
			
			String tableName = ContextServiceConfig.REGION_INFO_TABLE_NAME;
			
			String newTableCommand = "create table "+tableName+" ( regionKey INTEGER PRIMARY KEY ";
			
			//TODO: which indexing scheme is better, indexing two attribute once or creating a index over all 
			// attributes
			
			Iterator<String> attrIter = attributeMap.keySet().iterator();
			
			while( attrIter.hasNext() )
			{
				String attrName = attrIter.next();
				AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
				
				String attrDataType  = attrMetaInfo.getDataType();
				String mySQLDataType = AttributeTypes.mySQLDataType.get(attrDataType);
				// lower range of this attribute in this subspace
				String lowerAttrName = "lower"+attrName;
				String upperAttrName = "upper"+attrName;
				
				newTableCommand = newTableCommand + " , "+lowerAttrName+" "+mySQLDataType+" , "
									+upperAttrName+" "+mySQLDataType;
				
				if( ContextServiceConfig.sqlDBType == SQL_DB_TYPE.MYSQL )
				{
					newTableCommand = newTableCommand 
							+" , "+ "INDEX USING BTREE("+lowerAttrName+" , "+upperAttrName+")";
				}
			}
			
			newTableCommand = newTableCommand +" )";
			stmt.executeUpdate(newTableCommand);
			
	//				if( ContextServiceConfig.sqlDBType == SQL_DB_TYPE.SQLITE )
	//				{
	//					createIndexesInSQLite();
	//				}
				
		}
		catch( SQLException mysqlEx )
		{
			mysqlEx.printStackTrace();
		}
		finally
		{
			try
			{
				if( stmt != null )
					stmt.close();
				if( myConn != null )
					myConn.close();
			}
			catch( SQLException sqex )
			{
				sqex.printStackTrace();
			}
		}
	}
	
	
	@Override
	public List<Integer> getNodeIdsForSearch(String tableName, 
						HashMap<String, AttributeValueRange> attrValRangeMap)
	{
		String selectTableSQL = "SELECT regionKey from "+tableName+" WHERE ";
		
		Iterator<String> attrIter = attrValRangeMap.keySet().iterator();
		int count = 0;
		while(attrIter.hasNext())
		{
			String attrName = attrIter.next();
			AttributeValueRange attrValRange = attrValRangeMap.get(attrName);
			
			String lowerAttr = "lower"+attrName;
			String upperAttr = "upper"+attrName;
			
			AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
			String dataType = attrMetaInfo.getDataType();
			
			if(AttributeTypes.compareTwoValues(attrValRange.getLowerBound(),
					attrValRange.getUpperBound(), dataType))
			{
				String queryMin  =  AttributeTypes.convertStringToDataTypeForMySQL
							(attrValRange.getLowerBound(), dataType) + "";
				String queryMax  =  AttributeTypes.convertStringToDataTypeForMySQL
							(attrValRange.getUpperBound(), dataType) + "";
				
				// three cases to check, documentation
				// trying to find if there is an overlap in the ranges, 
				// the range specified by user and the range in database.
				// overlap is there if queryMin lies between the range in database
				// or queryMax lies between the range in database.
				// So, we specify two or conditions.
				// for right side value, it can't be equal to rangestart, 
				// but it can be equal to rangeEnd, although even then it doesn't include
				// rangeEnd.
				// or the range lies in between the queryMin and queryMax
				
				// follwing the convention that the in (lowerVal, upperVal) range lowerVal is included in 
				// range and upperVal is not included in range. This convnetion is for data storage in mysql
				// queryMin and queryMax aare always both end points included.
				// means a query >= queryMin and query <= queryMax, but never query > queryMin and query < queryMax
				selectTableSQL = selectTableSQL +" ( "
						+ "( "+lowerAttr+" <= "+queryMin +" AND "+upperAttr+" > "+queryMin+" ) OR "
						+ "( "+lowerAttr+" <= "+queryMax +" AND "+upperAttr+" > "+queryMax+" ) OR "
						+ "( "+lowerAttr+" >= "+queryMin +" AND "+upperAttr+" <= "+queryMax+" ) "+" ) ";
			}
			else // when lower value in query predicate is greater than upper value, meaning circular query, 
				// it is done mostly for generating uniform workload for experiments
			{
				// first case from lower to max value
				String queryMin  =  AttributeTypes.convertStringToDataTypeForMySQL
							(attrValRange.getLowerBound(), dataType) + "";
				String queryMax  =  AttributeTypes.convertStringToDataTypeForMySQL
							(attrMetaInfo.getMaxValue(), dataType) + "";
				
				selectTableSQL = selectTableSQL +"( ( "
						+ "( "+lowerAttr+" <= "+queryMin +" AND "+upperAttr+" > "+queryMin+" ) OR "
						+ "( "+lowerAttr+" <= "+queryMax +" AND "+upperAttr+" > "+queryMax+" ) OR "
						+ "( "+lowerAttr+" >= "+queryMin +" AND "+upperAttr+" <= "+queryMax+" ) "+" ) OR ";
				
				// second case from minvalue to upper val
				queryMin  =  AttributeTypes.convertStringToDataTypeForMySQL
								(attrMetaInfo.getMinValue(), dataType) + "";
				queryMax  =  AttributeTypes.convertStringToDataTypeForMySQL
								(attrValRange.getUpperBound(), dataType) + "";
				
				selectTableSQL = selectTableSQL +"( "
						+ "( "+lowerAttr+" <= "+queryMin +" AND "+upperAttr+" > "+queryMin+" ) OR "
						+ "( "+lowerAttr+" <= "+queryMax +" AND "+upperAttr+" > "+queryMax+" ) OR "
						+ "( "+lowerAttr+" >= "+queryMin +" AND "+upperAttr+" <= "+queryMax+" ) "+" )  )";
			}
			
			if( count != (attrValRangeMap.size()-1) )
			{
				selectTableSQL = selectTableSQL + " AND ";
			}
			count++;
		}
		
		Statement stmt 			= null;
		Connection myConn 		= null;
		List<Integer> nodeList 	= new LinkedList<Integer>();
		
		try
		{
			myConn = this.dataSource.getConnection(DB_REQUEST_TYPE.SELECT);
			stmt = myConn.createStatement();
			
			ResultSet rs = stmt.executeQuery(selectTableSQL);
			
		    while( rs.next() )
		    {
		    	//Retrieve by column name
		    	int regionKey  	 = rs.getInt("regionKey");
		    	nodeList.add(regionKey);
		    }
		    rs.close();
		} catch( SQLException sqlex )
		{
			sqlex.printStackTrace();
		}
		finally
		{
			try
			{
				if( stmt != null )
					stmt.close();
				
				if( myConn != null )
					myConn.close();
			}
			catch(SQLException sqlex)
			{
				sqlex.printStackTrace();
			}
		}
		return nodeList;
	}
	
	
	@Override
	public List<Integer> getNodeIdsForUpdate(String tableName, 
						HashMap<String, AttributeValueRange> attrValRangeMap)
	{
		String selectTableSQL = "SELECT regionKey from "+tableName+" WHERE ";
		
		Iterator<String> attrIter = attrValRangeMap.keySet().iterator();
		int count = 0;
		while(attrIter.hasNext())
		{
			String attrName = attrIter.next();
			AttributeValueRange attrValRange = attrValRangeMap.get(attrName);
			
			String lowerAttr = "lower"+attrName;
			String upperAttr = "upper"+attrName;
			
			
			AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
			String dataType = attrMetaInfo.getDataType();
			
			assert(AttributeTypes.compareTwoValues(attrValRange.getLowerBound(),
					attrValRange.getUpperBound(), dataType));
			{
				String attrVal  =  AttributeTypes.convertStringToDataTypeForMySQL
							(attrValRange.getLowerBound(), dataType) + "";
				
				selectTableSQL = selectTableSQL 
						+ "( "+lowerAttr+" <= "+attrVal +" AND "+upperAttr+" > "+attrVal+" ) ";
			}
			
			if( count != (attrValRangeMap.size()-1) )
			{
				selectTableSQL = selectTableSQL + " AND ";
			}
			count++;
		}
		
		Statement stmt 		= null;
		Connection myConn 	= null;
		List<Integer> nodeList = new LinkedList<Integer>();
		
		try
		{
			myConn = this.dataSource.getConnection(DB_REQUEST_TYPE.SELECT);
			stmt = myConn.createStatement();
			
			ResultSet rs = stmt.executeQuery(selectTableSQL);
			
		    while( rs.next() )
		    {
		    	//Retrieve by column name
		    	int regionKey  	 = rs.getInt("regionKey");
		    	nodeList.add(regionKey);
		    }
		    rs.close();
		} catch( SQLException sqlex )
		{
			sqlex.printStackTrace();
		}
		finally
		{
			try
			{
				if( stmt != null )
					stmt.close();
				
				if( myConn != null )
					myConn.close();
			}
			catch(SQLException sqlex)
			{
				sqlex.printStackTrace();
			}
		}
		return nodeList;
	}
	
	
	@Override
	public void insertRegionInfoIntoTable(String tableName, 
											RegionInfo regionInfo)
	{
		Connection myConn   	= null;
		Statement stmt      	= null;
		
		
		String insertTableSQL 	= "INSERT INTO "+tableName 
				+" ( regionKey ";
		
		HashMap<String, AttributeValueRange> valSpaceBoundary 
				= regionInfo.getValueSpaceInfo().getValueSpaceBoundary();
		
		Iterator<String> attrIter = valSpaceBoundary.keySet().iterator();
		int regionKey = regionInfo.getRegionKey();
		
		while(attrIter.hasNext())
		{
			String attrName = attrIter.next();
			
			String lowerAtt = "lower"+attrName;
			String upperAtt = "upper"+attrName;
			
			insertTableSQL = insertTableSQL + ", "+lowerAtt+" , "+upperAtt;
		}
		
		insertTableSQL = insertTableSQL + " ) VALUES ( "+regionKey;
		
		attrIter = valSpaceBoundary.keySet().iterator();
		
		while(attrIter.hasNext())
		{
			String attrName = attrIter.next();
			AttributeValueRange attrValRange = valSpaceBoundary.get(attrName);
			
			AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
			String dataType = attrMetaInfo.getDataType();
			
			String lowerBound  = AttributeTypes.convertStringToDataTypeForMySQL
						(attrValRange.getLowerBound(), dataType)+"";
			String upperBound  = AttributeTypes.convertStringToDataTypeForMySQL
						(attrValRange.getUpperBound(), dataType)+"";
			
			insertTableSQL = insertTableSQL + " , "+lowerBound+" , "+ 
					upperBound;
		}
		
		insertTableSQL = insertTableSQL + " ) ";
		
		try
		{
			myConn = this.dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
			stmt = myConn.createStatement();

			// execute insert SQL stetement
			stmt.executeUpdate(insertTableSQL);
			
		} catch(SQLException sqlex)
		{
			sqlex.printStackTrace();
		}
		finally
		{
			try
			{
				if( myConn != null )
				{
					myConn.close();
				}
				if( stmt != null )
				{
					stmt.close();
				}
			} catch(SQLException sqex)
			{
				sqex.printStackTrace();
			}
		}
	}
}