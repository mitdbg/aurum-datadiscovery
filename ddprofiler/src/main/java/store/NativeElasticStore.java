package store;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import core.WorkerTaskResult;
import core.config.ProfilerConfig;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.mapping.PutMapping;

public class NativeElasticStore implements Store {
	
	final private Logger LOG = LoggerFactory.getLogger(NativeElasticStore.class.getName());

	// Although native client, we still use Jest for put mappings, which happens only once
	private JestClient client;
	private JestClientFactory factory = new JestClientFactory();
	private String serverUrl;
	private String storeServer;
	private int storePort;
	
	private Client nativeClient;
	private BulkProcessor bulkProcessor;
	
	public NativeElasticStore(ProfilerConfig pc) { 
		String storeServer = pc.getString(ProfilerConfig.STORE_SERVER);
		int storePort = pc.getInt(ProfilerConfig.STORE_PORT);
		int storeHttpPort = pc.getInt(ProfilerConfig.STORE_HTTP_PORT);
		this.storeServer = storeServer;
		this.storePort = storePort;
		this.serverUrl = "http://"+storeServer+":"+storeHttpPort;
	}
	
	@Override
	public void initStore() {
		this.silenceJestLogger();
		factory.setHttpClientConfig(new HttpClientConfig
                .Builder(serverUrl)
                .multiThreaded(true)
                .build());
		client = factory.getObject();
		
		// Create the native client
		try {
			nativeClient = TransportClient.builder().build()
					.addTransportAddress(new InetSocketTransportAddress(
							InetAddress.getByName(storeServer), storePort));
		} 
		catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		
		// Create bulk processor
		bulkProcessor = BulkProcessor.builder(nativeClient, 
				new BulkProcessor.Listener() {
			
			private long startRequest;
			private long endRequest;
			
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
				startRequest = System.currentTimeMillis();
			}
			
			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
				endRequest = System.currentTimeMillis();
				LOG.info("Done bulk index request, took: {}", (endRequest - startRequest));
			}
			
			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				LOG.error("FAILED? " + request.toString());
				LOG.error(failure.getMessage());
			}
			
		})	
		.setBulkActions(-1)
		.setBulkSize(new ByteSizeValue(50, ByteSizeUnit.MB))
		.setFlushInterval(TimeValue.timeValueSeconds(5))
		.setConcurrentRequests(1) // Means requests are queud while a bulkrequest is in progress
		.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)) // just default 
		.build();
		
		// Create the appropriate mappings for the indices
		PutMapping textMapping = new PutMapping.Builder(
				"text",
				"column",
				"{ \"properties\" : "
				+ "{ \"id\" :   {\"type\" : \"integer\","
				+ 				"\"store\" : \"yes\","
				+ 				"\"index\" : \"not_analyzed\"},"
				+ "\"sourceName\" :   {\"type\" : \"string\","
				+ 				"\"index\" : \"not_analyzed\"}, "
				+ "\"columnName\" :   {\"type\" : \"string\","
				+ 				"\"index\" : \"not_analyzed\", "
				+				"\"ignore_above\" : 512 },"
				+ "\"text\" : {\"type\" : \"string\", "
				+ 				"\"store\" : \"no\"," // space saving?
				+ 				"\"index\" : \"analyzed\","
				+				"\"analyzer\" : \"english\","
				+ 				"\"term_vector\" : \"yes\"}"
				+ "}"
				+ " "
				+ "}"
		).build();
		System.out.println(textMapping.toString());
		
		PutMapping profileMapping = new PutMapping.Builder(
				"profile",
				"column",
				"{ \"properties\" : "
				+ "{ "
				+ "\"id\" : {\"type\" : \"integer\", \"index\" : \"not_analyzed\"},"
				+ "\"dbName\" : {\"type\" : \"string\", \"index\" : \"not_analyzed\"},"
				+ "\"sourceName\" : {\"type\" : \"string\", \"index\" : \"not_analyzed\"},"
				+ "\"columnNameNA\" : {\"type\" : \"string\", \"index\" : \"not_analyzed\"},"
				+ "\"columnName\" : {\"type\" : \"string\", "
				+ 		"\"index\" : \"analyzed\", "
				+ 		"\"analyzer\" : \"english\"},"
				+ "\"dataType\" : {\"type\" : \"string\", \"index\" : \"not_analyzed\"},"
				+ "\"totalValues\" : {\"type\" : \"integer\", \"index\" : \"not_analyzed\"},"
				+ "\"uniqueValues\" : {\"type\" : \"integer\", \"index\" : \"not_analyzed\"},"
				+ "\"entities\" : {\"type\" : \"string\", \"index\" : \"analyzed\"}," // array
				+ "\"minValue\" : {\"type\" : \"float\", \"index\" : \"not_analyzed\"},"
				+ "\"maxValue\" : {\"type\" : \"float\", \"index\" : \"not_analyzed\"},"
				+ "\"avgValue\" : {\"type\" : \"float\", \"index\" : \"not_analyzed\"},"
				+ "\"median\" : {\"type\" : \"long\", \"index\" : \"not_analyzed\"},"
				+ "\"iqr\" : {\"type\" : \"long\", \"index\" : \"not_analyzed\"}"
				+ "} }"
		).build();
		
		// Make sure the necessary elastic indexes exist and apply the mappings
		try {
			client.execute(new CreateIndex.Builder("text").build());
			client.execute(new CreateIndex.Builder("profile").build());
			client.execute(textMapping);
			client.execute(profileMapping);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean indexData(int id, String sourceName, String columnName, List<String> values) {
		String strId = Integer.toString(id);
		//String v = concatValues(values);
		
		XContentBuilder builder = null;
		try {
			builder = jsonBuilder()
					.startObject()
						.field("id", strId)
						.field("sourceName", sourceName)
						.field("columnName", columnName)
						.startArray("text");
						
						for (String v : values) {
							builder.value(v);
						}
						
						builder.endArray()
					.endObject();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
		// Using bulkProcessor
		IndexRequest ir = new IndexRequest("text", "column").source(builder);
		bulkProcessor.add(ir);
		
		return true;
	}
	
	
	public boolean _indexData(int id, String sourceName, String columnName, List<String> values) {
		String strId = Integer.toString(id);
		
		// TODO: monitor this, we are now indexing multi-values, hoping that termvectors
		// do what we want on the other side...
		//String v = concatValues(values);
		
		XContentBuilder builder = null;
		try {
			builder = jsonBuilder()
					.startObject()
						.field("id", strId)
						.field("sourceName", sourceName)
						.field("columnName", columnName)
						.startArray("text");
			
						for (String v : values) {
							builder.value(v);
						}
						
						builder.endArray()
					.endObject();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
		// Using bulkProcessor
		
		IndexRequest ir = new IndexRequest("text", "column", strId).source(builder);
		
		
		String upScript = "if (ctx._source.containsKey(\"text\")) "
				+ "ctx._source.text += moreText "
				+ "else "
				+ "ctx._source.text = moreText";
		
		Map<String, Object> params = new HashMap<>();
		params.put("moreText", values);
		UpdateRequest ur = new UpdateRequest("text", "column", strId);
		Script theScript = new Script(upScript, ScriptType.INLINE, null, params);
		
		ur.script(theScript);
		
		ur.upsert(ir);
		
		try {
			nativeClient.update(ur).get();
		} 
		catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		
//		bulkProcessor.add(ur);
		
		return true;
	}

	@Override
	public boolean storeDocument(WorkerTaskResult wtr) {
		String strId = Integer.toString(wtr.getId());
		
		XContentBuilder builder = null;
		try {
			builder = jsonBuilder()
					.startObject()
						.field("id", wtr.getId())
						.field("dbName", wtr.getDBName())
						.field("sourceName", wtr.getSourceName())
						.field("columnNameNA", wtr.getColumnName())
						.field("columnName", wtr.getColumnName())
						.field("dataType", wtr.getDataType())
						.field("totalValues", wtr.getTotalValues())
						.field("uniqueValues", wtr.getUniqueValues())
						.field("entities", wtr.getEntities().toString())
						.field("minValue", wtr.getMinValue())
						.field("maxValue", wtr.getMaxValue())
						.field("avgValue", wtr.getAvgValue())
						.field("median", wtr.getMedian())
						.field("iqr", wtr.getIQR())
					.endObject();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
		IndexRequest ir = new IndexRequest("profile", "column", strId).source(builder);
		
		bulkProcessor.add(ir);
		
		return true;
	}

	@Override
	public void tearDownStore() {
		client.shutdownClient();
		bulkProcessor.close();
		nativeClient.close();
		factory = null;
		client = null;
	}
	
	//TODO: do we need this?
	private String concatValues(List<String> values) {
		StringBuilder sb = new StringBuilder();
		String separator = " ";
		for(String s : values) {
			sb.append(s);
			sb.append(separator);
		}
		return sb.toString();
	}
	
	private void silenceJestLogger() {
		final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger("io.searchbox.client");
		final org.slf4j.Logger logger2 = org.slf4j.LoggerFactory.getLogger("io.searchbox.action");
		if (!(logger instanceof ch.qos.logback.classic.Logger)) {
		    return;
		}
		if (!(logger2 instanceof ch.qos.logback.classic.Logger)) {
		    return;
		}
		ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) logger;
		ch.qos.logback.classic.Logger logbackLogger2 = (ch.qos.logback.classic.Logger) logger2;
		logbackLogger.setLevel(ch.qos.logback.classic.Level.INFO);
		logbackLogger2.setLevel(ch.qos.logback.classic.Level.INFO);
	}

}
