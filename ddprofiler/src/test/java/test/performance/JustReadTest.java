package test.performance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import core.config.ProfilerConfig;
import inputoutput.Attribute;
import inputoutput.conn.Connector;
import inputoutput.conn.FileConnector;
import preanalysis.PreAnalyzer;
import preanalysis.Values;

public class JustReadTest {

	private String path = "C:\\csv\\";
	private String filename="dataset1.csv";
			
	//private String path = "/Users/ra-mit/Desktop/mitdwh_test/";
	//private String filename = "short_cis_course_catalog.csv";
	private String separator = ",";
	
	@Test
	public void test() throws IOException {
		Properties p = new Properties();
		p.setProperty(ProfilerConfig.NUM_POOL_THREADS, "8");
		int numRecordChunk = 500;
		p.setProperty(ProfilerConfig.NUM_RECORD_READ, "500");
		ProfilerConfig pc = new ProfilerConfig(p);
		
		int iterations = 100;
		long start = System.currentTimeMillis();
		while(iterations > 0) {
			Files.walk(Paths.get(path)).forEach(filePath -> {
				if (Files.isRegularFile(filePath)) {
					String name = filePath.getFileName().toString();
					FileConnector fc = null;
					try {
						fc = new FileConnector(path, name, separator);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					Connector c = fc;
					PreAnalyzer pa = new PreAnalyzer();
					pa.composeConnector(c);
					
					// Consume all remaining records from the connector
					Map<Attribute, Values> data = pa.readRows(numRecordChunk);
					int records = 0;
					while(data != null) {
						records = records + data.size();
						// Read next chunk of data
						data = pa.readRows(numRecordChunk);
					}
				}
			});
			iterations--;
		}
		long end = System.currentTimeMillis();
		System.out.println("Total time: " + (end-start));
	}
	
}
