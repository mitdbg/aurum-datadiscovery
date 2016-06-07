package test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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

	private String path = "/Users/ra-mit/Desktop/mitdwh_test/";
	private String filename = "short_cis_course_catalog.csv";
	private String separator = ",";
	
	@Test
	public void storeLoadingE2ETest() {
		
		Properties p = new Properties();
		p.setProperty(ProfilerConfig.NUM_POOL_THREADS, "8");
		p.setProperty(ProfilerConfig.NUM_RECORD_READ, "500");
		ProfilerConfig pc = new ProfilerConfig(p);
		
		// Create store
		Store elasticStore = StoreFactory.makeElasticStore(pc);
		//elasticStore.initStore();
		
		Conductor c = new Conductor(pc, elasticStore);
		c.start();
		try {
			Files.walk(Paths.get(path)).forEach(filePath -> {
			    if (Files.isRegularFile(filePath)) {
			    	String name = filePath.getFileName().toString();
			    	WorkerTask wt = WorkerTask.makeWorkerTaskForCSVFile(path, name, separator);
			    	c.submitTask(wt);
			    }
			});
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
//		WorkerTask wt = WorkerTask.makeWorkerTaskForCSVFile(path, filename, separator);
//		c.submitTask(wt);
		long start = System.currentTimeMillis();
		while(c.isTherePendingWork()) {
			List<WorkerTaskResult> results = null;
			do {
				results = c.consumeResults(); // we know there is only one set of results
			} while(results.isEmpty());
			
			for(WorkerTaskResult wtr : results) {
				elasticStore.storeDocument(wtr);
			}
		}
		long end = System.currentTimeMillis();
		
		System.out.println("DONE: " + (end-start));
	}

}
