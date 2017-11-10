package core.config.sources;

import java.io.File;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import core.SourceType;

public class YAMLParser {

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
	}

    }
}
