package core.config.sources;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import core.SourceType;

public class YAMLParser {

    public static List<SourceConfig> processSourceConfig(String path) {
	File file = new File(path);
	Sources srcs = null;
	ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
	try {
	    srcs = mapper.readValue(file, Sources.class);
	} catch (Exception e) {
	    e.printStackTrace();
	}
	int apiVersion = srcs.getApi_version();
	assert (apiVersion == 0); // to support api evolution

	List<SourceConfig> sourceConfigs = new ArrayList<>();

	// Parse every source in the file
	List<Source> sources = srcs.getSources();
	for (Source src : sources) {
	    String name = src.getName();
	    SourceType type = src.getType();
	    JsonNode props = src.getConfig();
	    SourceConfig sc = null;
	    if (type == SourceType.csv) {
		sc = mapper.convertValue(props, CSVSource.class);
	    } else if (type == SourceType.postgres) {
		sc = mapper.convertValue(props, PostgresSource.class);
	    } else {
		System.out.println("Unsupported!");
		System.exit(0);
	    }
	    sc.setSourceName(name);
	    sourceConfigs.add(sc);
	}

	return sourceConfigs;
    }

    public static void main(String args[]) {

	File file = new File("/Users/ra-mit/development/discovery_proto/ddprofiler/src/main/resources/template.yml");
	Sources srcs = null;
	ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
	try {
	    srcs = mapper.readValue(file, Sources.class);
	} catch (Exception e) {
	    e.printStackTrace();
	}

	System.out.println("api_version: " + srcs.getApi_version());
	List<Source> sources = srcs.getSources();
	for (Source src : sources) {
	    String name = src.getName();
	    SourceType type = src.getType();
	    JsonNode props = src.getConfig();
	    System.out.println("name: " + name);
	    System.out.println("type: " + type);
	    if (type == SourceType.csv) {
		CSVSource csvSource = mapper.convertValue(props, CSVSource.class);
		String path = csvSource.getPath();
		String separator = csvSource.getSeparator();
		System.out.println(path);
		System.out.println(separator);
	    }
	    if (type == SourceType.postgres) {
		PostgresSource postgresSource = mapper.convertValue(props, PostgresSource.class);
		String databaseName = postgresSource.getDatabase_name();
		String db_server_ip = postgresSource.getDb_server_ip();
		int db_server_port = postgresSource.getDb_server_port();
		String db_username = postgresSource.getDb_username();
		String db_password = postgresSource.getDb_password();
		System.out.println(databaseName);
		System.out.println(db_server_ip);
		System.out.println(db_server_port);
		System.out.println(db_username);
		System.out.println(db_password);
	    }
	}

    }
}
