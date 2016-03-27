package com.njp.learn.lucene.es1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.alibaba.fastjson.JSON;

public class SearchUtil {
	private static final Logger logger = Logger.getLogger(SearchUtil.class);

	private static final String[] fields = { "teacher_id", "teacher_name", "teacher_birth_year", "teacher_desc","teacher_address" };

	public static Object StringSearch(String query) {
		QueryBuilder qb = QueryBuilders.queryStringQuery(query);

		SearchResponse response = ESUtil.getInstance().prepareSearch("sanhao_dev").setTypes("teacher").setQuery(qb)
				.addFields(fields).setFrom(0).setSize(15).execute().actionGet();

		// You will get all individual responses from
		// MultiSearchResponse#getResponses()
		long nbHits = response.getHits().getTotalHits();

		logger.info("nbHits : " + nbHits);
		
		
		List<HashMap<String, String>> results = new ArrayList<HashMap<String,String>>();
		
		for (SearchHit hit : response.getHits().hits()) {
			logger.info(hit.field("teacher_id").getValue().toString() + " ==> "
					+ hit.field("teacher_name").getValue().toString() + " ==> "
					+ hit.field("teacher_desc").getValue().toString());
			
			HashMap<String, String> result = new HashMap<String,String>();
			for (String field : fields) {
				result.put(field, hit.field(field).getValue().toString());
			}
			
			logger.info(JSON.toJSON(result));
			
			results.add(result);

		}
		logger.info(JSON.toJSON(results));
		
		return JSON.toJSON(results);
	}
}
