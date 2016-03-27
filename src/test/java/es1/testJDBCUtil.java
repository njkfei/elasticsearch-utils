package es1;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.njp.learn.lucene.es1.JDBCUtil;

public class testJDBCUtil {
	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_URL = "jdbc:mysql://52.192.165.91:3306/sanhao_dev";
	// Database credentials
	private static final String USER = "root";
	private static final String PASS = "123456";
	static final String SQL = "SELECT * FROM ysyy_user WHERE id > ? limit ?";
	
	//@Test
/*	public void test1() throws ClassNotFoundException, SQLException{
		String prepare = "SELECT * FROM ysyy_user WHERE id > ? limit ?";
		int id = 10;
		int limit = 10;
		List<Map<String, Object>> result = JDBCUtil.getResult(prepare, id, limit);
		
		for(Map<String, Object> row : result){
			System.out.println(row.toString());
		}
		
		id = Integer.parseInt((String) result.get(result.size() -1).get("user_id"));
		
		System.out.println("-------------------------");
		result = JDBCUtil.getResult(prepare, id, limit);
		
		for(Map<String, Object> row : result){
			System.out.println(row.toString());
		}
		
		id = Integer.parseInt((String) result.get(result.size() -1).get("user_id"));
		
		System.out.println("-------------------------");
		result = JDBCUtil.getResult(prepare, id, limit);
		
		for(Map<String, Object> row : result){
			System.out.println(row.toString());
		}
		
		
		
	}*/
}
