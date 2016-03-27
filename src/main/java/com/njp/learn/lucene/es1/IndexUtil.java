package com.njp.learn.lucene.es1;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class IndexUtil {
	private static final Logger logger = Logger.getLogger(IndexUtil.class);

	public static void Indexer() throws InterruptedException{

		List<Map<String, Object>> result = JDBCUtil.getResult();
		
		logger.info("1 current size : " + result.size());
		
		long before = System.currentTimeMillis();
		
		while( result != null && !result.isEmpty()){
			
			
			long beforees = System.currentTimeMillis();
			
			logger.info("insert date to es start ....");
/*			for(Map<String, Object> row : result){
				 // logger.info(row.toString());
				ESUtil.insert(row);
			}*/
			
			if(false == ESUtil.insertBatch(result)){
				logger.info("insert date to es fail. stop...");
				return;
				
			}
			
			long endes = System.currentTimeMillis();
			
			logger.info("insert date to es ok .... total time : " + ( endes - beforees) + "ms");
		
			beforees = System.currentTimeMillis();
			
			result = JDBCUtil.getResult();
			endes = System.currentTimeMillis();
			//logger.info("3 current size : " + result.size());
			
			logger.info("get date from mysql ok .... total time : " + ( endes - beforees) + "ms");
		}
		
		long end = System.currentTimeMillis();
		
		logger.info("total time : " + ( end - before) / 1000);
	}
}
