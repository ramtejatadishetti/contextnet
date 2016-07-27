package edu.umass.cs.contextservice.database;

import java.sql.DriverManager;
import java.sql.SQLException;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

import edu.umass.cs.contextservice.logging.ContextServiceLogger;

public class SQLDBTesting 
{
	private static final String EMPLOYEE_TABLE = "create table MyEmployees3 ( "
		      + "   id INT PRIMARY KEY, firstName VARCHAR(20), lastName VARCHAR(20), "
		      + "   title VARCHAR(20), salary INT )";
	
	public static Connection getConnection() throws Exception 
	{
		String driver = "org.gjt.mm.mysql.Driver";
		String url = "jdbc:mysql://compute-0-13/contextDatabase";
		String username = "root";
		String password = "root";
		Class.forName(driver);
		Connection conn = (Connection) DriverManager.getConnection(url, username, password);
		return conn;
	}
	
	public static void main(String args[])
	{
		Connection conn = null;
		Statement stmt = null;
		try
		{
			conn = getConnection();
			stmt = (Statement) conn.createStatement();
			stmt.executeUpdate(EMPLOYEE_TABLE);
			stmt.executeUpdate("insert into MyEmployees3(id, firstName) values(100, 'A')");
			stmt.executeUpdate("insert into MyEmployees3(id, firstName) values(200, 'B')");
			ContextServiceLogger.getLogger().fine("CreateEmployeeTableMySQL: main(): table created.");
		} 
		catch (ClassNotFoundException e) 
		{
			ContextServiceLogger.getLogger().fine("error: failed to load MySQL driver.");
			e.printStackTrace();
		} catch (SQLException e) 
		{
			ContextServiceLogger.getLogger().fine("error: failed to create a connection object.");
		    e.printStackTrace();
		} catch (Exception e) 
		{
			ContextServiceLogger.getLogger().fine("other error:");
			e.printStackTrace();
		} finally 
		{
			try 
			{
				stmt.close();
		        conn.close();        
		    }
			catch (SQLException e) 
			{
		    	e.printStackTrace();
		    }
		}
	}
}