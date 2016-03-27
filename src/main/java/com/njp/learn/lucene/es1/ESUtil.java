package com.njp.learn.lucene.es1;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;

import com.google.gson.JsonObject;

/**
 * Hello world!
 *
 */
public class ESUtil {
	private static final Logger logger = Logger.getLogger(ESUtil.class);
	/*
	 * private String host; private int port;
	 */
	private static Client client;
	private static String index;
	private static String type;
	private static String host;
	private static int port = 9300;
	private static String url;
	private static boolean delete; // 是否删除
	
	private static int number_of_shards;
	private static int number_of_replicas;
	static {
		String config = "src/main/resources/conf/indextype.json";
		JsonObject object = FileUtil.getJson(config);

		index = object.get("index").getAsString();
		type = object.get("type").getAsString();
		host = object.get("host").getAsString();
		port = object.get("port").getAsInt();
		url = object.get("map").getAsString();
		number_of_shards = object.get("number_of_shards").getAsInt();
		number_of_replicas = object.get("number_of_replicas").getAsInt();
		delete = object.get("delete").getAsBoolean();
	}


	public static Client getInstance() {
		if (client == null) {
			logger.info("create elasticsearch client...");
			client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(host, port));

			// 如果索引存在，则删除索引，这条比较危险，如果有数据，则全部被删除了
			/*
			 * if(existsIndex(index)){ deleteIndex(index); }
			 * 
			 * // 创建索引
			 * client.admin().indices().prepareCreate(index).execute().actionGet
			 * ();
			 */
			// 如果type存在，则删除，新建全量索引
/*			if (existsType(index, type)) {
				deleteType(index, type);
				// 创建schema
				applyMapping(index, type, url);
			
			}*/
			
			if(false == existsIndex(index)){
				// 如果索引不存在，则创建索引
				logger.info("索引不存在，创建索引");
				// client.admin().indices().prepareCreate(index).execute().actionGet();
				creatIndex(index,number_of_shards,number_of_replicas);
			}
			
			if(false == existsType(index, type)){	
				// 创建schema
				applyMapping(index, type, url);
			}else if(delete == true){
				logger.info("create elasticsearch client...");
				deleteType(index, type);
				// 创建schema
				applyMapping(index, type, url);
			}
			
			return client;
		} else {
			return client;
		}
	}
	
	// 创建索引
	public static void creatIndex(String index,int number_of_shards,int number_of_replicas ){
			Map<String, Object> source = new HashMap<String,Object>();
			source.put("number_of_shards", number_of_shards);
			source.put("number_of_replicas", number_of_replicas);
			CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(index).setSource(source);
		    CreateIndexResponse resp = createIndexRequestBuilder.execute().actionGet();
		   
		    if(resp.isAcknowledged()){
		    	logger.info("创建索引成功　：" + index);
		    }else{
		    	logger.info("创建索引失败 : " + index);
		    }
		    
		    
	}
	
	// 创建索引和type
	public static boolean create(String index,String type,String url){
		// 如果type存在，则删除，新建全量索引
		if (existsType(index, type)) {
			deleteType(index, type);
			// 创建schema
			applyMapping(index, type, url);
			return true;
		}
		return true;
	}

	// 插入一条文档
	public static IndexResponse insert(Map<String, Object> data) {
		// IndexRequest request = new
		// IndexRequest(index).type(type).source(data);

		IndexResponse response = getInstance().prepareIndex(index, type).setSource(data).execute().actionGet();
		return response;
	}

	// 批量插入
	public static boolean insertBatch(List<Map<String, Object>> datas) {
		BulkRequestBuilder bulkRequest = getInstance().prepareBulk();

		for (Map<String, Object> data : datas) {
			//bulkRequest.add(getInstance().prepareIndex(index, type).setSource(data));
			bulkRequest.add(new IndexRequest(index).type(type).source(data));
		}
		// bulkRequest.add(new IndexRequest(index).type(type).source(data));

		BulkResponse resp = bulkRequest.execute().actionGet();

		if (resp.hasFailures()) {
			logger.info(" insertBatch error .........");
			logger.info(resp.buildFailureMessage());
			return false;
		}
		
		return true;
	}

	// 更新一条文档
	public static boolean update(int _id,Map<String,Object> data) throws InterruptedException{
		//IndexResponse response = getInstance().prepareUpdate().setSource(data).execute().actionGet();
		XContentBuilder jsonBuilde;
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index(index);
		updateRequest.type(type);
		updateRequest.id(Integer.toString(_id));
		
		UpdateResponse resp = null;
		try {
			jsonBuilde = XContentFactory.jsonBuilder();
			for(String key : data.keySet()){
			
			updateRequest.doc(jsonBuilde
			        .startObject()
			            .field(key, data.get(key))
			        .endObject());
			}
			resp = getInstance().update(updateRequest).actionGet();
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
		if(resp.isCreated()){
			return true;
		}else{
			return true;
		}
	 }
	 
	// 判断索引是否存在
	public static boolean existsIndex(String index) {
		return (getInstance().admin().indices().prepareExists(index).execute().actionGet().isExists());
	}

	// 判断typd是否存在
	public static boolean existsType(String index, String type) {

		TypesExistsResponse response = getInstance().admin().indices().prepareTypesExists(index).setTypes(type).execute()
				.actionGet();
		return response.isExists();
	}

	// 判断文档是否存在
	public static boolean existsDoc(String index,String type,int id){
		
		SearchResponse resp = getInstance().prepareSearch(index).setTypes(type)
		        .setQuery(QueryBuilders.matchQuery("_id", id))
		        .setSize(1).execute().actionGet();
		
		if(resp.getHits().totalHits() > 0){
			return true;
		}else{
			return false;
		}
	 }
	 

	// 删除索引
	public static boolean deleteIndex(String index) {

		DeleteIndexResponse delete = getInstance().admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
		if (!delete.isAcknowledged()) {
			logger.info("Index " + index + " wasn't deleted");
			return false;
		} else {
			logger.info("Index " + index + "  deleted ok ...");
			return true;
		}
	}

	// 删除type
	public static boolean deleteType(String index, String type) {
		DeleteMappingRequest deleteMapping = new DeleteMappingRequest(index).types(type);
		DeleteMappingResponse delete = getInstance().admin().indices().deleteMapping(deleteMapping).actionGet();

		if (!delete.isAcknowledged()) {
			logger.info("Index " + index + " Type :" + type + "wasn't deleted");
			return false;
		} else {
			logger.info("Index " + index + " Type :" + type + "  deleted ok ...");
			return true;
		}
	}
	
	// 删除document
	public static boolean deleteDocument(String index, String type,int _id){
		DeleteResponse delete = getInstance().prepareDelete("twitter", "tweet", "1").execute().actionGet();
		
		if (!delete.isFound()) {
			logger.info("Index " + index + " Type :" + type + " _id : " + _id + "wasn't deleted");
			return false;
		} else {
			logger.info("Index " + index + " Type :" + type + " _id : " + _id  + "  deleted ok ...");
			return true;
		}
	}

	// 根据配置文件设置索引和表
	public static void applyMapping(String index, String type, String location) {

		String source = null;
		try {
			source = FileUtil.readJsonDefn(location);
		} catch (Exception e) {
			
			e.printStackTrace();
		}

		// logger.info(source);

		if (source != null) {
			PutMappingRequest request = Requests.putMappingRequest(index).type(type).source(source);

			// Create type and mapping
			PutMappingResponse resp = getInstance().admin().indices().putMapping(request).actionGet();

			if (resp.isAcknowledged()) {
				logger.info(" appMapping ok");
			} else {
				logger.info(" appMapping fail");
			}
		}
	}

	
	//　显示仓库
/*	public static List listRepositories() {
	    GetRepositoriesRequestBuilder getRepo =
	            new GetRepositoriesRequestBuilder( getInstance().admin().cluster());
	    GetRepositoriesResponse repositoryMetaDatas = getRepo.execute().actionGet();
	    repositoryMetaDatas.repositories().
	    return repositoryMetaDatas.repositories()
	            .stream()
	            .map(Repository::mapFrom)
	            .collect(Collectors.toList());
	}*/
	
/*	public List showSnapshots(String repositoryName) {
	    GetSnapshotsRequestBuilder builder = 
	            new GetSnapshotsRequestBuilder(client.admin().cluster());
	    builder.setRepository(repositoryName);
	    GetSnapshotsResponse getSnapshotsResponse = builder.execute().actionGet();
	    return getSnapshotsResponse.getSnapshots().stream()
	            .map(SnapshotInfo::name)
	            .collect(Collectors.toList());
	}
	*/
	// 创建仓库
	private static void createSnapshotRepository() {
	    Settings settings = ImmutableSettings.builder()
	            .put("location", "/mount/backups/newrepo")
	            .put("compress","true")
	            .build();

	    PutRepositoryRequestBuilder putRepo = 
	    		new PutRepositoryRequestBuilder( getInstance().admin().cluster());
	    PutRepositoryResponse resp = putRepo.setName("newrepo")
	            .setType("fs")
	            .setSettings(settings)
	            .execute().actionGet();
	    
	    if(resp.isAcknowledged()){
	    	logger.info("创建仓库成功");
	    }else{
	    	logger.info("创建仓库失败");
	    }

	}
	
	public static  void deleteRepository(String repositoryName) {
	    DeleteRepositoryRequestBuilder builder = 
	            new DeleteRepositoryRequestBuilder(getInstance().admin().cluster());
	    builder.setName(repositoryName);
	    DeleteRepositoryResponse  resp = builder.execute().actionGet();
	   
	    if(resp.isAcknowledged()){
	    	logger.info("删除仓库成功");
	    }else{
	    	logger.info("删除仓库失败");
	    }
	    
	}
	
	//　创建快照
	public static void createSnapshot(String repositoryName, String snapshotPrefix, String indices) {
	    CreateSnapshotRequestBuilder builder = new CreateSnapshotRequestBuilder(getInstance().admin().cluster());

	    System.out.println( DateTimeUtil.getYYYY_MM_DD());
	    String snapshot = snapshotPrefix + "_" + DateTimeUtil.getYYYY_MM_DD();
	    builder.setRepository(repositoryName)
	            .setIndices(indices)
	            .setSnapshot(snapshot);
	    CreateSnapshotResponse resp = builder.execute().actionGet();
	    
	    if(resp.status() != RestStatus.INTERNAL_SERVER_ERROR  ){
	    	logger.info("创建快照成功");
	    }else{
	    	logger.info("创建快照失败");
	    }
	}
	
	// 删除快照
	public static void deleteSnapshot(String repositoryName, String snapshot) {
	    DeleteSnapshotRequestBuilder builder = new DeleteSnapshotRequestBuilder(getInstance().admin().cluster());
	    builder.setRepository(repositoryName).setSnapshot(snapshot);
	    DeleteSnapshotResponse resp =  builder.execute().actionGet();
	    
	    if(resp.isAcknowledged() ){
	    	logger.info("删除快照成功");
	    }else{
	    	logger.info("删除快照失败");
	    }
	}
	
	// 恢复快照
	public static void restoreSnapshot(String repositoryName, String snapshot) {
	    // Obtain the snapshot and check the indices that are in the snapshot
	    GetSnapshotsRequestBuilder builder = new GetSnapshotsRequestBuilder(getInstance().admin().cluster());
	    builder.setRepository(repositoryName);
	    builder.setSnapshots(snapshot);
	    GetSnapshotsResponse resp = builder.execute().actionGet();
	    
	    // Check if the index exists and if so, close it before we can restore it.
	    ImmutableList indices = resp.getSnapshots().get(0).indices();
	    CloseIndexRequestBuilder closeIndexRequestBuilder =
	            new CloseIndexRequestBuilder(client.admin().indices());
	    closeIndexRequestBuilder.setIndices((String[]) indices.toArray(new String[indices.size()]));
	    closeIndexRequestBuilder.execute().actionGet();

	    // Now execute the actual restore action
	    RestoreSnapshotRequestBuilder restoreBuilder = new RestoreSnapshotRequestBuilder(getInstance().admin().cluster());
	    restoreBuilder.setRepository(repositoryName).setSnapshot(snapshot);
	    RestoreSnapshotResponse resp1 =  restoreBuilder.execute().actionGet();

	    if(resp1.status() !=  RestStatus.INTERNAL_SERVER_ERROR ){
	    	logger.info("恢复快照成功");
	    	for(String list : (String[]) indices.toArray(new String[indices.size()])){
	    		logger.info("恢复的快照名为:" + list);
	    	}
	    }else{
	    	logger.info("恢复快照失败");
	    }
	}

	public static void main(String[] args)
			throws InterruptedException, ExecutionException, ElasticsearchException, IOException {
		// try {
		// applyMapping(index,type,"src/main/resources/conf/schema.json");
		// } catch (Exception e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

		// deleteType(index,type);
		
	//	logger.info(existsDoc("sanhao_dev","teacher",45));
/*		
		Map<String, Object> data = new HashMap<String,Object>();
		data.put("teacher_nickname", "聂金平测试修改");
		update(45,data);*/
		
		//createSnapshotRepository();
		
		// deleteRepository("my_backup");
		//createSnapshot("newrepo","njp","sanhao_dev");
		
		//deleteSnapshot("newrepo","njp_2015-12-08");
		
		// deleteIndex("sanhao_dev");
		
		 //creatIndex("sanhao_dev",5,0);
		//restoreSnapshot("newrepo","njp-1");
	}
}
