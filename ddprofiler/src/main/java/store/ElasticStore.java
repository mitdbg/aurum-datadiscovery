package store;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import core.WorkerTaskResult;
import core.config.ProfilerConfig;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.mapping.PutMapping;


public class ElasticStore implements Store {

	private JestClient client;
	private JestClientFactory factory = new JestClientFactory();
	private String serverUrl;
	
	public ElasticStore(ProfilerConfig pc) { 
		String storeServer = pc.getString(ProfilerConfig.STORE_SERVER);
		int storePort = pc.getInt(ProfilerConfig.STORE_PORT);
		this.serverUrl = "http://"+storeServer+":"+storePort;
	}
	
	@Override
	public void initStore() {
		this.silenceJestLogger();
		factory.setHttpClientConfig(new HttpClientConfig
                .Builder(serverUrl)
                .multiThreaded(true)
                .build());
		client = factory.getObject();
		
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
				+ "\"sourceName\" : {\"type\" : \"string\", \"index\" : \"not_analyzed\"},"
				//+ "\"columnName\" : {\"type\" : \"string\", \"index\" : \"analyzed\"},"
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
	public boolean indexData(int id,String sourceName, String columnName, List<String> values) {
		String strId = Integer.toString(id);
		Map<String, String> source = new LinkedHashMap<String,String>();
		String v = concatValues(values);
		source.put("id", strId);
		source.put("sourceName", sourceName);
		source.put("columnName", columnName);
		source.put("text", v);
		Index index = new Index.Builder(source).index("text").type("column").build();
		try {
			client.execute(index);
		} 
		catch (IOException e) {
			return false;
		}
		return true;
	}

	@Override
	public boolean storeDocument(WorkerTaskResult wtr) {
		String strId = Integer.toString(wtr.getId());
		Map<String, String> source = new LinkedHashMap<String,String>();
		source.put("id", Integer.toString(wtr.getId()));
		source.put("sourceName", wtr.getSourceName());
		source.put("columnName", wtr.getColumnName());
		source.put("dataType", wtr.getDataType());
		source.put("totalValues", Integer.toString(wtr.getTotalValues()));
		source.put("uniqueValues", Integer.toString(wtr.getUniqueValues()));
		source.put("entities", wtr.getEntities().toString());
		source.put("minValue", Float.toString(wtr.getMinValue()));
		source.put("maxValue", Float.toString(wtr.getMaxValue()));
		source.put("avgValue", Float.toString(wtr.getAvgValue()));
		source.put("median", Long.toString(wtr.getMedian()));
		source.put("iqr", Long.toString(wtr.getIQR()));
		Index index = new Index.Builder(source).index("profile").type("column").id(strId).build();
		try {
			client.execute(index);
		} 
		catch (IOException e) {
			return false;
		}
		return true;
	}

	@Override
	public void tearDownStore() {
		client.shutdownClient();
		factory = null;
		client = null;
	}
	
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
