package com.njp.learn.lucene.es1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mysql.jdbc.ResultSetMetaData;

public class JDBCUtil {
	private static final Logger logger = Logger.getLogger(JDBCUtil.class);
	// JDBC driver name and database URL
	private static  String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static  String DB_URL = "jdbc:mysql://52.192.165.91:3306/sanhao_dev";
	// Database credentials
	private static  String USER = "root";
	private static  String PASS = "123456";
	private static String _id = "id";
	private static int start_id;        //建索引时的起始ID
	private static int current_id;      //　建索引时的数据库的当前id
	private static int count;           // 建数据库时单次读的记录数
	// SQL
	private static  String SQL = "SELECT * FROM ysyy_user WHERE ? > ? limit ?";
	
	private static Map<String,String> schema;

	static {
		String config="src/main/resources/db/teacher.db";
		JsonObject object = FileUtil.getJson(config);
		
		JDBC_DRIVER = object.get("jdbc").getAsString();
		DB_URL = object.get("url").getAsString();
		USER = object.get("username").getAsString();
		PASS = object.get("password").getAsString();
		_id = object.get("_id").getAsString();
		SQL = object.get("sql").getAsString();
		
		start_id = object.get("start_id").getAsInt();
		count = object.get("count").getAsInt();
		current_id = start_id;
				
		schema = new HashMap<String,String>();
		
		object.get("schema").getAsJsonArray();
		
		for(JsonElement elem : object.get("schema").getAsJsonArray() ){
			schema.put(elem.getAsJsonObject().get("name").getAsString(), elem.getAsJsonObject().get("type").getAsString());
		}

/*		schema.put("user_id","int");
		schema.put("user_account","long");
		schema.put("user_salt","int");
		schema.put("user_level","int");
		schema.put("user_recommend","int");
		schema.put("user_type","int");
		schema.put("channel_id","int");

		schema.put("user_balance","double");*/

	}

	private static Connection conn;

	public static Connection getInstance() {
		if (conn == null) {
			try {
				Class.forName(JDBC_DRIVER);
				// STEP 3: Open a connection
				logger.info("Connecting to database : " + DB_URL);
				conn = DriverManager.getConnection(DB_URL, USER, PASS);
				
				if(conn == null){
					logger.info("连接失败　db_url : " + DB_URL);
				}else{
					logger.info("数据库连接成功　db_url : " + DB_URL);
				}
				
				return conn;
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		} else {
			return conn;
		}

	}

	private static java.sql.Timestamp getCurrentTimeStamp() {

		java.util.Date today = new java.util.Date();
		return new java.sql.Timestamp(today.getTime());

	}

	

	public static List<Map<String,Object>> getResult() {
		   List<Map<String, Object>> result = new ArrayList<Map<String,Object>>();
		   
		   try{
		   PreparedStatement  preparedStatement = getInstance().prepareStatement(SQL);
		 
		   preparedStatement.setInt(1,current_id);
		   preparedStatement.setInt(2, count);
		   
		  // logger.debug(preparedStatement.toString());
		   
		   ResultSet rs = preparedStatement.executeQuery();
		   
		   while(rs.next()){
			   Map<String, Object> row = new HashMap<String, Object>();
			   /*	
			    * 				ResultSetMetaData rsmd = (ResultSetMetaData) rs.getMetaData();

				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					String key = rsmd.getColumnName(i);
					String value = rs.getString(i);
					value = rs.get
					row.put(key, value);
				}*/
				
			   // 设置 　_id字段
			   current_id = rs.getInt(_id);
				
				for(String key : schema.keySet()){
					if(schema.get(key).equals("integer")){
						row.put(key, rs.getInt(key));
					}else if(schema.get(key).equals("long")){
						row.put(key, rs.getLong(key));
					}else if(schema.get(key).equals("double")){
						row.put(key, rs.getDouble(key));
					}
					else{
						row.put(key, rs.getString(key));
					}
				}
				
				result.add(row);
		   }
		   }catch(Exception e){
			   e.printStackTrace();
			   
			   return null;
		   }
		   
		   logger.info(" current id : " + current_id);
		   
		   return result;
	   }

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Connection conn = JDBCUtil.getInstance();
		Statement stmt = null;
		try {

			// STEP 4: Execute a query
			logger.info("Creating statement...");
			stmt = conn.createStatement();
			String sql;
			sql = "SELECT * FROM `ysyy_user_teacher` WHERE `teacher_id` > 0 limit 100";
			ResultSet rs = stmt.executeQuery(sql);

			logger.info("result size : " + rs.getFetchSize() );
			// STEP 5: Extract data from result set
			while (rs.next()) {
				// Retrieve by column name
				int id = rs.getInt("user_id");
				// int age = rs.getInt("user_account");
				Map<String, String> object = new HashMap<String, String>();
				ResultSetMetaData rsmd = (ResultSetMetaData) rs.getMetaData();

				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					String key = rsmd.getColumnName(i);
					String value = rs.getString(i);
					object.put(key, value);
				}

				logger.info(object.toString());

			}
			// STEP 6: Clean-up environment
			rs.close();
			stmt.close();

		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			} // nothing we can do

		} // end try
		logger.info("Goodbye!");
	}
}
