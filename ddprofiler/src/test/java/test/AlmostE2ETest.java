package test;

import java.util.List;
import java.util.Properties;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import core.Conductor;
import core.WorkerTask;
import core.WorkerTaskResult;
import core.config.ProfilerConfig;

public class AlmostE2ETest {

	private String path = "/Users/ra-mit/Desktop/mitdwhdata/";
	private String filename = "short_cis_course_catalog.csv";
	private String separator = ",";
	
	private ObjectMapper om = new ObjectMapper();
	
	@Test
	public void almostE2ETest() {
		
		Properties p = new Properties();
		p.setProperty(ProfilerConfig.NUM_POOL_THREADS, "1");
		p.setProperty(ProfilerConfig.NUM_RECORD_READ, "500");
		ProfilerConfig pc = new ProfilerConfig(p);
		
		Conductor c = new Conductor(pc, null);
		
		c.start();
		
		WorkerTask wt = WorkerTask.makeWorkerTaskForCSVFile(path, filename, separator);
		c.submitTask(wt);
		
		List<WorkerTaskResult> results = null;
		do {
			results = c.consumeResults(); // we know there is only one set of results
		} while(results.isEmpty());
		
		for(WorkerTaskResult wtr : results) {
			String textual = null;
			try {
				textual = om.writeValueAsString(wtr);
			} 
			catch (JsonProcessingException e) {
				e.printStackTrace();
			}
			System.out.println(textual);
		}
		
	}

}
