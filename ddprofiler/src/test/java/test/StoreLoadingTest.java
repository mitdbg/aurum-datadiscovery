package test;

import java.util.List;
import java.util.Properties;

import org.junit.Test;

import core.Conductor;
import core.WorkerTask;
import core.WorkerTaskResult;
import core.config.ProfilerConfig;
import store.Store;
import store.StoreFactory;

public class StoreLoadingTest {

	private String path = "/Users/ra-mit/Desktop/mitdwhdata/";
	private String filename = "short_cis_course_catalog.csv";
	private String separator = ",";
	
	@Test
	public void storeLoadingE2ETest() {
		
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
		
		// Create store
		Store elasticStore = StoreFactory.makeElasticStore(pc);
		
		elasticStore.initStore();
		
		for(WorkerTaskResult wtr : results) {
			elasticStore.storeDocument(wtr);
		}
		
		System.out.println("DONE!");
	}

}
