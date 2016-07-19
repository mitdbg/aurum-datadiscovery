package core;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analysis.modules.EntityAnalyzer;
import core.config.ProfilerConfig;
import opennlp.tools.namefind.TokenNameFinderModel;
import store.Store;

public class Conductor {
	
	final private Logger LOG = LoggerFactory.getLogger(Conductor.class.getName());

	private ProfilerConfig pc;
	private File errorLogFile;
	
	private BlockingQueue<WorkerTask> taskQueue;
	private ExecutorService pool;
	private List<Future<List<WorkerTaskResult>>> futures;
	private BlockingQueue<WorkerTaskResult> results;
	
	private boolean used = false;
	
	private Store store;
	
	private Thread consumer;
	private Consumer runnable;
	// Cached entity analyzers (expensive initialization)
	private Map<String, EntityAnalyzer> cachedEntityAnalyzers;
	
	public Conductor(ProfilerConfig pc, Store s) {
		this.pc = pc;
		this.store = s;
		this.taskQueue = new LinkedBlockingQueue<>();
		this.futures = new ArrayList<>();
		this.results = new LinkedBlockingQueue<>();
		int numWorkers = pc.getInt(ProfilerConfig.NUM_POOL_THREADS);
		List<String> uniqueThreadNames = new ArrayList<>();
		cachedEntityAnalyzers = new HashMap<>();
		List<TokenNameFinderModel> modelList = new ArrayList<>();
		for(int i = 0; i < numWorkers; i++) {
			String name = "Thread-"+new Integer(i).toString();
			uniqueThreadNames.add(name);
			if(modelList.isEmpty()) {
				EntityAnalyzer first = new EntityAnalyzer();
				cachedEntityAnalyzers.put(name, first); // pay cost of loading model
				modelList = first.getCachedModelList();
			}
			else{
				cachedEntityAnalyzers.put(name, new EntityAnalyzer(modelList));
			}
		}
		this.pool = Executors.newFixedThreadPool(numWorkers, new DDThreadFactory(uniqueThreadNames));
		LOG.info("Create worker pool, num workers: {}", numWorkers);
		this.runnable = new Consumer();
		this.consumer = new Thread(runnable);
		String errorLogFileName = pc.getString(ProfilerConfig.ERROR_LOG_FILE_NAME);
		this.errorLogFile = new File(errorLogFileName);
	}
	
	public void start() {
		this.store.initStore();
		this.consumer.start();
	}
	
	public void stop() {
		this.runnable.stop();
	}
	
	public boolean submitTask(WorkerTask task) {
		LOG.info("Task {} submitted for processing", task.getTaskId());
		return taskQueue.add(task);
	}
	
	public boolean isTherePendingWork() {
		return !used || futures.size() > 0;
	}
	
	public List<WorkerTaskResult> consumeResults() {
		List<WorkerTaskResult> availableResults = new ArrayList<>();
		WorkerTaskResult wtr = null;
		do {
			try {
				wtr = results.poll(500, TimeUnit.MILLISECONDS);
				if(wtr != null) {
					availableResults.add(wtr);
				}
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		} while(wtr != null);
		return availableResults;
	}
	
	class Consumer implements Runnable {

		private boolean doWork = true;
		
		public void stop() {
			doWork = false;
		}
		
		@Override
		public void run() {
			
			while(doWork) {
				
				// Attempt to consume new task
				WorkerTask wt = null;
				try {
					wt = taskQueue.poll(500, TimeUnit.MILLISECONDS);
				}
				catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				if(wt != null) {
					// Create worker to handle the task and submit to the pool
					Worker w = new Worker(wt, store, pc, cachedEntityAnalyzers);
					Future<List<WorkerTaskResult>> future = pool.submit(w);
					// Store future
					futures.add(future);
					used = true;
				}
				
				// Check if there are futures that have finished at this point
				Iterator<Future<List<WorkerTaskResult>>> it = futures.iterator();
				while(it.hasNext()) {
					Future<List<WorkerTaskResult>> f = it.next();
					if(f.isDone()) {
						try {
							LOG.info("Remaining futures: {}", futures.size());
							if(f.get() != null)
								results.addAll(f.get());
							it.remove();
						} 
						catch (InterruptedException e) {
							e.printStackTrace();
							it.remove(); // to make sure we make progress
						}
						catch (ExecutionException e) {
							Throwable t = e.getCause();
							String msg = t.getMessage();
							Utils.appendLineToFile(errorLogFile, msg);
							it.remove(); // to make sure we make progress
							LOG.warn(msg);
						}
					}
					else if(f.isCancelled()) {
						LOG.warn("The task was cancelled: unknown reason");
						it.remove();
					}
				}
			}
		}
	}

}
