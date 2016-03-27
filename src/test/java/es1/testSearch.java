package es1;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Ignore;
import org.junit.Test;

import com.njp.learn.lucene.es1.ESUtil;



public class testSearch {
	private static final Logger logger = Logger.getLogger(testSearch.class);

	@Ignore
	@Test
	public void testStringSearch() {
		SearchRequestBuilder srb1 = ESUtil.getInstance().prepareSearch().setQuery(QueryBuilders.queryStringQuery("李文"))
				.setSize(10);
		SearchRequestBuilder srb2 = ESUtil.getInstance().prepareSearch("sanhao_dev").setTypes("teacher")
				.setQuery(QueryBuilders.matchQuery("teacher_name", "聂")).setSize(10);

		MultiSearchResponse sr = ESUtil.getInstance().prepareMultiSearch().add(srb1).add(srb2).execute().actionGet();

		// You will get all individual responses from
		// MultiSearchResponse#getResponses()
		long nbHits = 0;
		for (MultiSearchResponse.Item item : sr.getResponses()) {
			SearchResponse response = item.getResponse();
			nbHits += response.getHits().getTotalHits();

			logger.info("nbHits : " + nbHits);

			for (SearchHit hit : response.getHits().hits()) {
				logger.info(hit.sourceAsString());
			}

		}
	}

	@Ignore
	@Test
	public void testQueryField() {
		SearchRequestBuilder srb1 = ESUtil.getInstance().prepareSearch()
				.setQuery(QueryBuilders.multiMatchQuery("聂", "_id", "teacher_name")).setSize(10);

		MultiSearchResponse sr = ESUtil.getInstance().prepareMultiSearch().add(srb1)
				// .add(srb2)
				.execute().actionGet();

		// You will get all individual responses from
		// MultiSearchResponse#getResponses()
		long nbHits = 0;
		for (MultiSearchResponse.Item item : sr.getResponses()) {
			SearchResponse response = item.getResponse();
			nbHits += response.getHits().getTotalHits();

			logger.info("nbHits : " + nbHits);

			for (SearchHit hit : response.getHits().hits()) {
				logger.info(hit.sourceAsString());
			}

		}

	}

	@Ignore
	@Test
	public void testTermSearch() {

		QueryBuilder qb = QueryBuilders.commonTermsQuery("teacher_name", "李");

		SearchResponse response = ESUtil.getInstance().prepareSearch("sanhao_dev").setTypes("teacher").setQuery(qb).addFields("teacher_id","teacher_name","teacher_desc")
				.setFrom(100).setSize(5)
				.execute().actionGet();

		// You will get all individual responses from
		// MultiSearchResponse#getResponses()
		long nbHits = 0;

		nbHits += response.getHits().getTotalHits();

		logger.info("nbHits : " + nbHits);

		for (SearchHit hit : response.getHits().hits()) {
			logger.info(hit.field("teacher_id").getValue().toString() + " ==> " + hit.field("teacher_name").getValue().toString() + " ==> " + hit.field("teacher_desc").getValue().toString());
			logger.info(hit.getId());
		}
	}
	
	@Ignore
	@Test
	public void testTermSearchBySort() {

		QueryBuilder qb = QueryBuilders.queryStringQuery( "好");

		SearchResponse response = ESUtil.getInstance().prepareSearch("sanhao_dev").setTypes("teacher").setQuery(qb).addFields("teacher_id","teacher_name","teacher_desc","teacher_pageview")
				.setFrom(1).setSize(15).addSort("teacher_pageview", SortOrder.DESC )
				.execute().actionGet();

		// You will get all individual responses from
		// MultiSearchResponse#getResponses()
		long nbHits = 0;

		nbHits += response.getHits().getTotalHits();

		logger.info("nbHits : " + nbHits);

		for (SearchHit hit : response.getHits().hits()) {
			logger.info(hit.field("teacher_id").getValue().toString() + " ==> " + hit.field("teacher_name").getValue().toString() + " ==> " + hit.field("teacher_pageview").getValue().toString());
			logger.info(hit.getId());
		}
	}
	
	@Ignore
	@Test
	public void testRangeSearch() {

		QueryBuilder qb = QueryBuilders.rangeQuery("teacher_birth_year").from(1985).to(1989).includeLower(true).includeUpper(false);

		SearchResponse response = ESUtil.getInstance().prepareSearch("sanhao_dev").setTypes("teacher").setQuery(qb).addFields("teacher_id","teacher_name","teacher_birth_year","teacher_desc")
				.setFrom(100).setSize(5)
				.execute().actionGet();

		// You will get all individual responses from
		// MultiSearchResponse#getResponses()
		long nbHits = 0;

		nbHits += response.getHits().getTotalHits();

		logger.info("nbHits : " + nbHits);

		for (SearchHit hit : response.getHits().hits()) {
			logger.info(hit.field("teacher_id").getValue().toString() + " ==> " + hit.field("teacher_birth_year").getValue().toString() + " ==> " + hit.field("teacher_name").getValue().toString() + " ==> " + hit.field("teacher_desc").getValue().toString());
		}
	}
	
	@Ignore
	@Test
	public void testMatchAllSearch() {

		QueryBuilder qb = QueryBuilders.matchAllQuery();

		SearchResponse response = ESUtil.getInstance().prepareSearch("sanhao_dev").setTypes("teacher").setQuery(qb).addFields("teacher_id","teacher_name","teacher_birth_year","teacher_desc")
				.setFrom(100).setSize(5)
				.execute().actionGet();

		// You will get all individual responses from
		// MultiSearchResponse#getResponses()
		long nbHits = 0;

		nbHits += response.getHits().getTotalHits();

		logger.info("nbHits : " + nbHits);

		for (SearchHit hit : response.getHits().hits()) {
			logger.info(hit.field("teacher_id").getValue().toString() + " ==> " + hit.field("teacher_birth_year").getValue().toString() + " ==> " + hit.field("teacher_name").getValue().toString() + " ==> " + hit.field("teacher_desc").getValue().toString());
		}
	}
	
	@Ignore
	@Test
	public void testIdsSearch() {

		QueryBuilder qb = QueryBuilders.idsQuery()
			    .addIds("45","46");

		SearchResponse response = ESUtil.getInstance().prepareSearch("sanhao_dev").setTypes("teacher").setQuery(qb).addFields("teacher_id","teacher_name","teacher_birth_year","teacher_desc")
				.setFrom(100).setSize(5)
				.execute().actionGet();

		// You will get all individual responses from
		// MultiSearchResponse#getResponses()
		long nbHits = 0;

		nbHits += response.getHits().getTotalHits();

		logger.info("nbHits : " + nbHits);

		for (SearchHit hit : response.getHits().hits()) {
			logger.info(hit.getId());
			logger.info(hit.field("teacher_id").getValue().toString() + " ==> " + hit.field("teacher_birth_year").getValue().toString() + " ==> " + hit.field("teacher_name").getValue().toString() + " ==> " + hit.field("teacher_desc").getValue().toString());
		}
	}

	// Disjunction Max Query
	@Ignore
	@Test
	public void testDisMaxSearch() {
		QueryBuilder qb = QueryBuilders.termQuery("teacher_name", "李");
		QueryBuilder qb1 = QueryBuilders.termQuery("teacher_desc", "李");
		
		QueryBuilder qb2 = QueryBuilders.disMaxQuery().add(qb).add(qb1).boost(0.1f).tieBreaker(0.7f);

		SearchResponse response = ESUtil.getInstance().prepareSearch("sanhao_dev").setTypes("teacher").setQuery(qb).addFields("teacher_id","teacher_name","teacher_birth_year","teacher_desc")
				.setFrom(100).setSize(15)
				.execute().actionGet();

		// You will get all individual responses from
		// MultiSearchResponse#getResponses()
		long nbHits = 0;

		nbHits += response.getHits().getTotalHits();

		logger.info("nbHits : " + nbHits);

		for (SearchHit hit : response.getHits().hits()) {
			logger.info(hit.field("teacher_id").getValue().toString() + " ==> " + hit.field("teacher_birth_year").getValue().toString() + " ==> " + hit.field("teacher_name").getValue().toString() + " ==> " + hit.field("teacher_desc").getValue().toString());
			logger.info(hit.score());
			logger.info(hit.getType());
		}
	}
	
	// fuzzy Query
	@Ignore
	@Test
	public void testDisFuzzySearch() {
		QueryBuilder qb = QueryBuilders.fuzzyQuery("teacher_name", "李文");

		SearchResponse response = ESUtil.getInstance().prepareSearch("sanhao_dev").setTypes("teacher").setQuery(qb).addFields("teacher_id","teacher_name","teacher_birth_year","teacher_desc")
				.setFrom(100).setSize(15)
				.execute().actionGet();

		// You will get all individual responses from
		// MultiSearchResponse#getResponses()
		long nbHits = 0;

		nbHits += response.getHits().getTotalHits();

		logger.info("nbHits : " + nbHits);

		for (SearchHit hit : response.getHits().hits()) {
			logger.info(hit.field("teacher_id").getValue().toString() + " ==> " + hit.field("teacher_birth_year").getValue().toString() + " ==> " + hit.field("teacher_name").getValue().toString() + " ==> " + hit.field("teacher_desc").getValue().toString());
			logger.info(hit.score());
			logger.info(hit.getType());
		}
	}
	
	
	//　Match All Query
	@Ignore
	@Test
	public void testMatchAllQuery(){
		QueryBuilder qb = QueryBuilders.matchAllQuery();
		SearchResponse response = ESUtil.getInstance().prepareSearch("sanhao_dev").setTypes("teacher").setQuery(qb).addFields("teacher_id","teacher_name","teacher_birth_year","teacher_desc")
				.setFrom(100).setSize(15)
				.execute().actionGet();

		// You will get all individual responses from
		// MultiSearchResponse#getResponses()
		long nbHits = 0;

		nbHits += response.getHits().getTotalHits();

		logger.info("nbHits : " + nbHits);

		for (SearchHit hit : response.getHits().hits()) {
			logger.info(hit.field("teacher_id").getValue().toString() + " ==> " + hit.field("teacher_birth_year").getValue().toString() + " ==> " + hit.field("teacher_name").getValue().toString() + " ==> " + hit.field("teacher_desc").getValue().toString());
			logger.info(hit.score());
			logger.info(hit.getType());
		}
	}
	
	// Prefix Query
	@Ignore
	@Test
	public void testPrefixQuery(){
		QueryBuilder qb = QueryBuilders.prefixQuery("teacher_name","聂");
		// QueryBuilder qb = QueryBuilders.prefixQuery("teacher_name","聂元");
		SearchResponse response = ESUtil.getInstance().prepareSearch("sanhao_dev").setTypes("teacher").setQuery(qb).addFields("teacher_id","teacher_name","teacher_birth_year","teacher_desc")
				.setFrom(0).setSize(15)
				.execute().actionGet();

		// You will get all individual responses from
		// MultiSearchResponse#getResponses()
		long nbHits = 0;

		nbHits += response.getHits().getTotalHits();

		logger.info("nbHits : " + nbHits);

		for (SearchHit hit : response.getHits().hits()) {
			logger.info(hit.field("teacher_id").getValue().toString() + " ==> " + hit.field("teacher_birth_year").getValue().toString() + " ==> " + hit.field("teacher_name").getValue().toString() + " ==> " + hit.field("teacher_desc").getValue().toString());
			logger.info(hit.score());
			logger.info(hit.getType());
		}
	}
	
	// Wildcard  Query
	//@Ignore
	@Test
	public void testWildcardQuery(){
		QueryBuilder qb = QueryBuilders.wildcardQuery("teacher_name","聂");
		SearchResponse response = ESUtil.getInstance().prepareSearch("sanhao_dev").setTypes("teacher").setQuery(qb).addFields("teacher_id","teacher_name","teacher_birth_year","teacher_desc")
				.setFrom(0).setSize(15)
				.execute().actionGet();

		// You will get all individual responses from
		// MultiSearchResponse#getResponses()
		long nbHits = 0;

		nbHits += response.getHits().getTotalHits();

		logger.info("nbHits : " + nbHits);

		for (SearchHit hit : response.getHits().hits()) {
			logger.info(hit.field("teacher_id").getValue().toString() + " ==> " + hit.field("teacher_birth_year").getValue().toString() + " ==> " + hit.field("teacher_name").getValue().toString() + " ==> " + hit.field("teacher_desc").getValue().toString());
			logger.info(hit.score());
			logger.info(hit.getType());
		}
	}
}
