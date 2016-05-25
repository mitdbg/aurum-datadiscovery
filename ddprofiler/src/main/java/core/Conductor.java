package core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import core.config.ProfilerConfig;
import store.Store;

public class Conductor {

	private ProfilerConfig pc;
	
	private BlockingQueue<WorkerTask> taskQueue;
	private ExecutorService pool;
	private List<Future<List<WorkerTaskResult>>> futures;
	private BlockingQueue<WorkerTaskResult> results;
	
	private Store store;
	
	private Thread consumer;
	private Consumer runnable;
	
	public Conductor(ProfilerConfig pc, Store s) {
		this.pc = pc;
		this.store = s;
		taskQueue = new LinkedBlockingQueue<>();
		futures = new ArrayList<>();
		results = new LinkedBlockingQueue<>();
		pool = Executors.newFixedThreadPool(pc.getInt(ProfilerConfig.NUM_POOL_THREADS));
		runnable = new Consumer();
		consumer = new Thread(runnable);
	}
	
	public void start() {
		this.consumer.start();
	}
	
	public void stop() {
		this.runnable.stop();
	}
	
	public boolean submitTask(WorkerTask task) {
		return taskQueue.add(task);
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
					Worker w = new Worker(wt, pc);
					Future<List<WorkerTaskResult>> future = pool.submit(w);
					// Store future
					futures.add(future);
				}
				
				// Check if there are futures that have finished at this point
				for(Future<List<WorkerTaskResult>> f : futures) {
					if(f.isDone()) {
						try {
							results.addAll(f.get());
						} 
						catch (InterruptedException | ExecutionException e) {
							e.printStackTrace();
						}
					}
					else if(f.isCancelled()) {
						// TODO: handle error somehow
					}
				}
			}
		}
	}

}
