package edu.umass.cs.contextservice.database.datasource;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class AbstractDataSource 
{	
	public static enum DB_REQUEST_TYPE {UPDATE, SELECT};
	
	public abstract Connection getConnection(DB_REQUEST_TYPE dbReqType) throws SQLException;
}