package store;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import core.WorkerTaskResult;
import core.config.ProfilerConfig;

public class NativeElasticStore implements Store {

    final private Logger LOG = LoggerFactory.getLogger(NativeElasticStore.class.getName());

    private String serverUrl;
    private String storeServer;
    private int storePort;

    private TransportClient client;
    // private Client nativeClient;
    private BulkProcessor bulkProcessor;

    public NativeElasticStore(ProfilerConfig pc) {
	String storeServer = pc.getString(ProfilerConfig.STORE_SERVER);
	int storePort = pc.getInt(ProfilerConfig.STORE_PORT);
	int storeHttpPort = pc.getInt(ProfilerConfig.STORE_HTTP_PORT);
	this.storeServer = storeServer;
	this.storePort = storePort;
	this.serverUrl = "http://" + storeServer + ":" + storeHttpPort;
    }

    @Override
    public void initStore() {
	// Create native client
	try {
	    client = new PreBuiltTransportClient(Settings.EMPTY)
		    .addTransportAddress(new TransportAddress(InetAddress.getByName(storeServer), storePort));
	} catch (UnknownHostException e) {
	    e.printStackTrace();
	}

	// Create bulk processor
	bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {

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

	}).setBulkActions(-1).setBulkSize(new ByteSizeValue(50, ByteSizeUnit.MB))
		.setFlushInterval(TimeValue.timeValueSeconds(5)).setConcurrentRequests(1) // Means requests are queud
											  // while a bulkrequest is in
											  // progress
		.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)) // just default
		.build();

	String settings = "{"

		+ "\"analysis\": {" + "\"char_filter\": {" + "\"_to-\": {" + "\"type\": \"mapping\","
		+ "\"mappings\": [\"_=>-\"]" + "}" + "},"

		+ "\"char_filter\": {" + "\"csv_to_none\": {" + "\"type\": \"mapping\"," + "\"mappings\": [\".csv=> \"]"
		+ "}" + "},"

		+ "\"filter\": {" + "\"english_stop\": {" + "\"type\": \"stop\"," + "\"stopwords\": \"_english_\""
		+ "}," + "\"english_stemmer\": {" + "	\"type\": \"stemmer\"," + "	\"language\": \"english\""
		+ "}," + "\"english_possessive_stemmer\": {" + "	\"type\": \"stemmer\","
		+ "	\"language\": \"possessive_english\"" + "}" + "},"

		+ "\"analyzer\": {" + "\"aurum_analyzer\": {" + "\"tokenizer\": \"standard\","
		+ "\"char_filter\": [\"_to-\", \"csv_to_none\"],"
		+ "\"filter\": [\"english_possessive_stemmer\", \"lowercase\", \"english_stop\", \"english_stemmer\"]"
		+ "}" + "}" + "}" // closes analysis
		+ "}"; // closes object

	// Create mapping for the indices
	// index 'text' type 'column'
	String textMapping = "{ \"properties\" : "

		+ "{ \"id\" :   {\"type\" : \"long\"," + "\"store\" : \"yes\"," + "\"index\" : \"not_analyzed\"},"
		+ "\"dbName\" :   {\"type\" : \"string\"," + "\"index\" : \"not_analyzed\"}, "
		+ "\"path\" :   {\"type\" : \"string\"," + "\"index\" : \"not_analyzed\"}, "
		+ "\"sourceName\" :   {\"type\" : \"string\"," + "\"index\" : \"not_analyzed\"}, "
		+ "\"columnName\" :   {\"type\" : \"string\"," + "\"index\" : \"not_analyzed\", "
		+ "\"ignore_above\" : 512 }," + "\"text\" : {\"type\" : \"string\", " + "\"store\" : \"no\"," // space
													      // saving?
		+ "\"index\" : \"analyzed\"," + "\"analyzer\" : \"english\"," + "\"term_vector\" : \"yes\"}" + "}" + " "
		+ "}";

	// index 'profile' type 'column'
	String profileMapping = "{ \"properties\" : "

		+ "{ \"id\" : {\"type\" : \"long\", \"index\" : \"not_analyzed\"},"
		+ "\"dbName\" : {\"type\" : \"string\", \"index\" : \"not_analyzed\"},"
		+ "\"path\" : {\"type\" : \"string\", \"index\" : \"not_analyzed\"},"
		+ "\"sourceNameNA\" : {\"type\" : \"string\", \"index\" : \"not_analyzed\"},"
		+ "\"sourceName\" : {\"type\" : \"string\"," + "\"index\" : \"analyzed\", "
		+ "\"analyzer\" : \"aurum_analyzer\"},"
		+ "\"columnNameNA\" : {\"type\" : \"string\", \"index\" : \"not_analyzed\"},"
		+ "\"columnName\" : {\"type\" : \"string\", " + "\"index\" : \"analyzed\", "
		+ "\"analyzer\" : \"aurum_analyzer\"},"

		+ "\"dataType\" : {\"type\" : \"string\", \"index\" : \"not_analyzed\"},"
		+ "\"totalValues\" : {\"type\" : \"integer\", \"index\" : \"not_analyzed\"},"
		+ "\"uniqueValues\" : {\"type\" : \"integer\", \"index\" : \"not_analyzed\"},"
		+ "\"entities\" : {\"type\" : \"string\", \"index\" : \"analyzed\"}," // array
		+ "\"minhash\" : {\"type\" : \"long\", \"index\" : \"not_analyzed\"}," // array
		+ "\"minValue\" : {\"type\" : \"float\", \"index\" : \"not_analyzed\"},"
		+ "\"maxValue\" : {\"type\" : \"float\", \"index\" : \"not_analyzed\"},"
		+ "\"avgValue\" : {\"type\" : \"float\", \"index\" : \"not_analyzed\"},"
		+ "\"median\" : {\"type\" : \"long\", \"index\" : \"not_analyzed\"},"
		+ "\"iqr\" : {\"type\" : \"long\", \"index\" : \"not_analyzed\"}" + "} }";

	// Create indexes and apply settings and mappings
	IndicesAdminClient admin = client.admin().indices();

	admin.prepareCreate("text");
	// admin.preparePutMapping("text").setType("column").setSource(textMapping).get();
	admin.prepareCreate("profile");
	// .setSettings(settings).get();
	// admin.preparePutMapping("profile").setType("column").setSource(profileMapping);
    }

    @Override
    public boolean indexData(long id, String dbName, String path, String sourceName, String columnName,
	    List<String> values) {

	XContentBuilder builder = null;
	try {
	    builder = jsonBuilder().startObject().field("id", id).field("dbName", dbName).field("path", path)
		    .field("sourceName", sourceName).field("columnName", columnName).startArray("text");

	    for (String v : values) {
		builder.value(v);
	    }

	    builder.endArray().endObject();
	} catch (IOException e) {
	    e.printStackTrace();
	}

	// Using bulkProcessor
	IndexRequest ir = new IndexRequest("text", "column").source(builder);
	bulkProcessor.add(ir);

	return true;
    }

    @Override
    public boolean storeDocument(WorkerTaskResult wtr) {
	String strId = Long.toString(wtr.getId());

	XContentBuilder builder = null;
	try {
	    builder = jsonBuilder().startObject().field("id", wtr.getId()).field("dbName", wtr.getDBName())
		    .field("path", wtr.getPath()).field("sourceName", wtr.getSourceName())
		    .field("columnNameNA", wtr.getColumnName()).field("columnName", wtr.getColumnName())
		    .field("dataType", wtr.getDataType()).field("totalValues", wtr.getTotalValues())
		    .field("uniqueValues", wtr.getUniqueValues()).field("entities", wtr.getEntities().toString())

		    .startArray("minhash");

	    long[] mh = wtr.getMH();
	    if (mh != null) { // that's an integer column
		for (long i : mh) {
		    builder.value(i);
		}
	    } else {
		builder.value(-1);
	    }

	    builder.endArray()

		    .field("minValue", wtr.getMinValue()).field("maxValue", wtr.getMaxValue())
		    .field("avgValue", wtr.getAvgValue()).field("median", wtr.getMedian()).field("iqr", wtr.getIQR())
		    .endObject();
	} catch (IOException e) {
	    e.printStackTrace();
	}

	IndexRequest ir = new IndexRequest("profile", "column", strId).source(builder);

	bulkProcessor.add(ir);

	return true;
    }

    @Override
    public void tearDownStore() {
	bulkProcessor.close();
    }

}
