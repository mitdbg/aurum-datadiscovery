package store;

import java.io.IOException;
import java.util.List;

import core.WorkerTaskResult;
import core.config.ProfilerConfig;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
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
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storeDocument(WorkerTaskResult wtr) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void tearDownStore() {
		client.shutdownClient();
		factory = null;
		client = null;
	}

}
