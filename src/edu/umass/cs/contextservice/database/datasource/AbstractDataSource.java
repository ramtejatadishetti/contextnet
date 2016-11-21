package edu.umass.cs.contextservice.database.datasource;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class AbstractDataSource 
{	
	public abstract Connection getConnection() throws SQLException;
}