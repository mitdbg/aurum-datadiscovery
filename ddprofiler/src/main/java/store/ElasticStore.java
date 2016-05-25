package store;

import java.util.List;

import core.WorkerTaskResult;
import core.config.ProfilerConfig;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

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
	}

	@Override
	public boolean indexData(List<String> values) {
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
