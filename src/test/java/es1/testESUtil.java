package es1;

import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.njp.learn.lucene.es1.ESUtil;

public class testESUtil {
	private static ESUtil util;
	
	@BeforeClass
	public void before(){
		String host = "52.192.165.91";
		int port = 9300;
		String index = "index_test";
		String type = "type_test";
		util = new ESUtil();		
	}
	
	@Test
	public void testInsert(){
		Map<String,Object> data = new HashMap<String,Object>();
		data.put("name", "njp");
		
		//util.insert(data);
	}
}
