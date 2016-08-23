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
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analysis.modules.EntityAnalyzer;
import core.TaskPackage.TaskPackageType;
import core.config.ProfilerConfig;
import opennlp.tools.namefind.TokenNameFinderModel;
import store.Store;

public class Conductor {
	
	final private Logger LOG = LoggerFactory.getLogger(Conductor.class.getName());

	private ProfilerConfig pc;
	private File errorLogFile;
	
	private BlockingQueue<TaskPackage> taskQueue;
	@Deprecated
	private ExecutorService pool;
	private List<Thread> workerPool;
	@Deprecated
	private List<Future<List<WorkerTaskResult>>> futures;
	private BlockingQueue<WorkerTaskResult> results;
	private BlockingQueue<ErrorPackage> errorQueue;
		
	private Store store;
	
	private Thread consumer;
	private Consumer runnable;
	// Cached entity analyzers (expensive initialization)
	private Map<String, EntityAnalyzer> cachedEntityAnalyzers;
	
	// Metrics
	private int totalTasksSubmitted = 0;
	private int totalFailedTasks = 0;
	private AtomicInteger totalProcessedTasks = new AtomicInteger();
	private AtomicInteger totalColumns = new AtomicInteger();
	
	public Conductor(ProfilerConfig pc, Store s) {
		this.pc = pc;
		this.store = s;
		this.taskQueue = new LinkedBlockingQueue<>();
		this.futures = new ArrayList<>();
		this.results = new LinkedBlockingQueue<>();
		this.errorQueue = new LinkedBlockingQueue<>();
		
		int numWorkers = pc.getInt(ProfilerConfig.NUM_POOL_THREADS);
		this.workerPool = new ArrayList<>();
		List<TokenNameFinderModel> modelList = new ArrayList<>();
		List<String> modelNameList = new ArrayList<>();
		EntityAnalyzer first = new EntityAnalyzer();
		modelList = first.getCachedModelList();
		modelNameList = first.getCachedModelNameList();
		for(int i = 0; i < numWorkers; i++) {
			String name = "Worker-"+new Integer(i).toString();
			EntityAnalyzer cached = new EntityAnalyzer(modelList, modelNameList);
			Worker w = new Worker(this, pc, name, taskQueue, errorQueue, store, cached);
			Thread t = new Thread(w, name);
			workerPool.add(t);
		}
		
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
	
	public boolean submitTask(TaskPackage task) {
		totalTasksSubmitted++;
		return taskQueue.add(task);
	}
	
	public boolean isTherePendingWork() {
		return this.totalProcessedTasks.get() < this.totalTasksSubmitted;
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
	
	public void notifyProcessedTask(int numCols) {
		totalProcessedTasks.incrementAndGet();
		LOG.info(" {}/{} ", totalProcessedTasks, totalTasksSubmitted);
		LOG.info(" Failed tasks: {} ", totalFailedTasks);
		totalColumns.addAndGet(numCols);
		LOG.info("Added: {} cols, total processed: {} ", numCols, totalColumns);
		LOG.info("");
	}
		
	class Consumer implements Runnable {

		private boolean doWork = true;
		
		public void stop() {
			doWork = false;
		}
		
		@Override
		public void run() {
			
			// Start workers
			for(Thread worker : workerPool) {
				worker.start();
			}
			
			while(doWork) {
				
				ErrorPackage ep;
				try{
					ep = errorQueue.poll(1000, TimeUnit.MILLISECONDS);
					if(ep != null) {
						String msg = ep.getErrorLog();
						Utils.appendLineToFile(errorLogFile, msg);
						LOG.warn(msg);
						totalProcessedTasks.incrementAndGet(); // other processed/failed task
						totalFailedTasks++;
					}
				}
				catch(InterruptedException e) {
					e.printStackTrace();
				}
				
//				// Check if there are futures that have finished at this point
//				Iterator<Future<List<WorkerTaskResult>>> it = futures.iterator();
//				while(it.hasNext()) {
//					Future<List<WorkerTaskResult>> f = it.next();
//					if(f.isDone()) {
//						try {
//							totalProcessedTasks++;
//							LOG.info(" {}/{} ", totalProcessedTasks, totalTasksSubmitted);
//							//LOG.info("Remaining futures: {}", futures.size());
//							if(f.get() != null) {
//								List<WorkerTaskResult> wtResults = f.get();
//								int numColumnsInResult = wtResults.size();
//								totalColumns += numColumnsInResult;
//								LOG.info("Added: {} cols, total processed: {} ", numColumnsInResult, totalColumns);
//								results.addAll(wtResults);
//							}
//							it.remove();
//						} 
//						catch (InterruptedException e) {
//							e.printStackTrace();
//							it.remove(); // to make sure we make progress
//						}
//						catch (ExecutionException e) {
//							Throwable t = e.getCause();
//							String msg = t.getMessage();
//							Utils.appendLineToFile(errorLogFile, msg);
//							it.remove(); // to make sure we make progress
//							LOG.warn(msg);
//						}
//					}
//					else if(f.isCancelled()) {
//						LOG.warn("The task was cancelled: unknown reason");
//						it.remove();
//					}
//				}
			}
		}
	}

}
