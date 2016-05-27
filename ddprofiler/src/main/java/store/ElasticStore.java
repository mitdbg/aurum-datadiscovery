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
		factory.setHttpClientConfig(new HttpClientConfig
                .Builder(serverUrl)
                .multiThreaded(true)
                .build());
		client = factory.getObject();
		
		// Create the appropriate mappings for the indices
		PutMapping textMapping = new PutMapping.Builder(
				"text",
				"column",
				"{ \"document\" : { \"properties\" : "
				+ "{ \"id\" :   {\"type\" : \"integer\", "
				+ 				"\"store\" : \"yes\","
				+ 				"\"index\" : \"not_analyzed\"} "
				+ "},"
				+ "{ \"text\" : {\"type\" : \"string\",  "
				+ 				"\"store\" : \"no\"," // space saving?
				+ 				"\"index\" : \"analyzed\","
				+ 				"\"term_vector\" : \"yes\"}"
				+ "}"
				+ " "
				+ "} }"
		).build();
		
		PutMapping profileMapping = new PutMapping.Builder(
				"profile",
				"column",
				"{ \"document\" : { \"properties\" : "
				+ "{ \"id\" : {\"type\" : \"integer\"} },"
				+ "{ \"sourceName\" : {\"type\" : \"string\"} },"
				+ "{ \"columnName\" : {\"type\" : \"string\"} },"
				+ "{ \"dataType\" : {\"type\" : \"string\"} },"
				+ "{ \"totalValues\" : {\"type\" : \"integer\"} },"
				+ "{ \"uniqueValues\" : {\"type\" : \"integer\"} },"
				+ "{ \"entities\" }," // array
				+ "{ \"minValue\" : {\"type\" : \"integer\"} },"
				+ "{ \"maxValue\" : {\"type\" : \"integer\"} },"
				+ "{ \"avgValue\" : {\"type\" : \"float\"} },"
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
	public boolean indexData(int id, List<String> values) {
		Map<String, String> source = new LinkedHashMap<String,String>();
		String v = concatValues(values);
		source.put("id", Integer.toString(id));
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
		Index index = new Index.Builder(source).index("profile").type("column").build();
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

}
