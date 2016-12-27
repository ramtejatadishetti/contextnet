package edu.umass.cs.contextservice.regionmapper;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ExecutorService;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.config.ContextServiceConfig.SQL_DB_TYPE;
import edu.umass.cs.contextservice.configurator.AbstractSubspaceConfigurator;
import edu.umass.cs.contextservice.configurator.BasicSubspaceConfigurator;
import edu.umass.cs.contextservice.database.datasource.AbstractDataSource;
import edu.umass.cs.contextservice.database.datasource.AbstractDataSource.DB_REQUEST_TYPE;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.regionmapper.AbstractRegionMappingPolicy.REQUEST_TYPE;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;



public class HyperdexBasedRegionMappingPolicy extends AbstractRegionMappingPolicy
{
	private final ExecutorService nodeES;
	private final AbstractDataSource dataSource;
	private final int numberAttrsPerSubspace;
	private final AbstractSubspaceConfigurator subspaceConfigurator;
	
	
	public HyperdexBasedRegionMappingPolicy( HashMap<String, AttributeMetaInfo> attributeMap, 
			CSNodeConfig csNodeConfig, AbstractDataSource dataSource, 
			int numberAttrsPerSubspace, ExecutorService nodeES )
	{
		super(attributeMap, csNodeConfig);
		
		this.dataSource = dataSource;
		this.numberAttrsPerSubspace = numberAttrsPerSubspace;
		this.nodeES = nodeES;
		
		subspaceConfigurator 
			= new BasicSubspaceConfigurator(csNodeConfig, numberAttrsPerSubspace, dataSource);
	}
	
	
	@Override
	public List<Integer> getNodeIDsForAValueSpace(ValueSpaceInfo valueSpace, 
			REQUEST_TYPE requestType) 
	{
		return null;
	}

	@Override
	public void computeRegionMapping() 
	{
		subspaceConfigurator.configureSubspaceInfo();
		HashMap<Integer, Vector<SubspaceInfo>> subspaceInfoMap 
					= subspaceConfigurator.getSubspaceInfoMap();
		
		createDBTables(subspaceInfoMap);
		
		subspaceConfigurator.generateAndStoreSubspacePartitionsInDB
													(nodeES);	
	}
	
	
	private void createDBTables(HashMap<Integer, Vector<SubspaceInfo>> subspaceInfoMap)
	{
		Connection myConn  = null;
		Statement  stmt    = null;	
		try
		{
			myConn = dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
			stmt   = myConn.createStatement();
			Iterator<Integer> subspaceIter = subspaceInfoMap.keySet().iterator();
			
			while( subspaceIter.hasNext() )
			{
				int subspaceId = subspaceIter.next();
				Vector<SubspaceInfo> replicasOfSubspace 
										= subspaceInfoMap.get(subspaceId);
				
				for(int i = 0; i<replicasOfSubspace.size(); i++)
				{
					SubspaceInfo subInfo = replicasOfSubspace.get(i);
					
					int replicaNum = subInfo.getReplicaNum();
					
					HashMap<String, AttributePartitionInfo> subspaceAttributes 
															= subInfo.getAttributesOfSubspace();
					
					// partition info storage info
					String tableName = "subspaceId"+subspaceId+"RepNum"+replicaNum+"PartitionInfo";
					
					String newTableCommand = "create table "+tableName+" ( hashCode INTEGER PRIMARY KEY , "
						      + "   respNodeID INTEGER ";
					
					//	      + ", upperRange DOUBLE NOT NULL, nodeID INT NOT NULL, "
					//	      + "   partitionNum INT AUTO_INCREMENT, INDEX USING BTREE (lowerRange, upperRange) )";
					//TODO: which indexing scheme is better, indexing two attribute once or creating a index over all 
					// attributes
					Iterator<String> attrIter = subspaceAttributes.keySet().iterator();
					while( attrIter.hasNext() )
					{
						String attrName = attrIter.next();
						String attrDataType  = subspaceAttributes.get(attrName).getAttrMetaInfo().getDataType();
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
					
				}
			}
			
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
	
	/**
	 * Returns subspace number of the maximum overlapping
	 * subspace. Used in processing search query.
	 * @return
	 */
	protected int getMaxOverlapSubspace( HashMap<String, ProcessingQueryComponent> pqueryComponents, 
			HashMap<String, ProcessingQueryComponent> matchingAttributes )
	{
		// first the maximum matching subspace is found and then any of its replica it chosen
		Iterator<Integer> keyIter   	= subspaceConfigurator.getSubspaceInfoMap().keySet().iterator();
		int maxMatchingAttrs 			= 0;
		
		HashMap<Integer, Vector<MaxAttrMatchingStorageClass>> matchingSubspaceHashMap = 
				new HashMap<Integer, Vector<MaxAttrMatchingStorageClass>>();
		
		while( keyIter.hasNext() )
		{
			int subspaceId = keyIter.next();
			SubspaceInfo currSubInfo = subspaceConfigurator.getSubspaceInfoMap().get(subspaceId).get(0);
			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo = currSubInfo.getAttributesOfSubspace();
			
			int currMaxMatch = 0;
			HashMap<String, ProcessingQueryComponent> currMatchingComponents 
						= new HashMap<String, ProcessingQueryComponent>();
			
			Iterator<String> attrIter = pqueryComponents.keySet().iterator();
			
			while( attrIter.hasNext() )
			{
				String attrName = attrIter.next();
				ProcessingQueryComponent pqc = pqueryComponents.get(attrName);
				if( attrsSubspaceInfo.containsKey(pqc.getAttributeName()) )
				{
					currMaxMatch = currMaxMatch + 1;
					currMatchingComponents.put(pqc.getAttributeName(), pqc);
				}
			}
			
			if(currMaxMatch >= maxMatchingAttrs)
			{
				maxMatchingAttrs = currMaxMatch;
				MaxAttrMatchingStorageClass maxAttrMatchObj = new MaxAttrMatchingStorageClass();
				maxAttrMatchObj.currMatchingComponents = currMatchingComponents;
				maxAttrMatchObj.subspaceId = subspaceId;
				
				if(matchingSubspaceHashMap.containsKey(currMaxMatch))
				{
					matchingSubspaceHashMap.get(currMaxMatch).add(maxAttrMatchObj);
				}
				else
				{
					Vector<MaxAttrMatchingStorageClass> currMatchingSubspaceNumVector 
													= new Vector<MaxAttrMatchingStorageClass>();
					currMatchingSubspaceNumVector.add(maxAttrMatchObj);
					matchingSubspaceHashMap.put(currMaxMatch, currMatchingSubspaceNumVector);
				}
			}
		}
		
		Vector<MaxAttrMatchingStorageClass> maxMatchingSubspaceNumVector 
			= matchingSubspaceHashMap.get(maxMatchingAttrs);
		
		int returnIndex = new Random().nextInt( maxMatchingSubspaceNumVector.size() );
		matchingAttributes.clear();
		
		Iterator<String> attrIter 
				= maxMatchingSubspaceNumVector.get(returnIndex).currMatchingComponents.keySet().iterator();
		while(attrIter.hasNext())
		{
			String attrName = attrIter.next();
			matchingAttributes.put( attrName, 
			maxMatchingSubspaceNumVector.get(returnIndex).currMatchingComponents.get(attrName) );
		}
		
		
		String print = "size "+maxMatchingSubspaceNumVector.size()+" ";
		for(int i=0;i<maxMatchingSubspaceNumVector.size();i++)
		{
			print = print + maxMatchingSubspaceNumVector.get(i).subspaceId+" ";
		}
		print = print + " chosen "+maxMatchingSubspaceNumVector.get(returnIndex).subspaceId;
		
		return maxMatchingSubspaceNumVector.get(returnIndex).subspaceId;
	}
	
	protected class MaxAttrMatchingStorageClass
	{
		public int subspaceId;
		public HashMap<String, ProcessingQueryComponent> currMatchingComponents;
	}
}
