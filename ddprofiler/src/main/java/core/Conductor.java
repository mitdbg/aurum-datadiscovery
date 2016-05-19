package core;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import core.config.ProfilerConfig;

public class Conductor {

	private ProfilerConfig pc;
	
	private BlockingQueue<WorkerTask> taskQueue;
	private ExecutorService pool;
	private List<Future<WorkerTaskResultFuture>> resultQueue;
	
	private Thread consumer;
	private Consumer runnable;
	
	public Conductor(ProfilerConfig pc) {
		this.pc = pc;
		taskQueue = new LinkedBlockingQueue<>();
		pool = Executors.newFixedThreadPool(pc.getInt(ProfilerConfig.NUM_POOL_THREADS));
		runnable = new Consumer();
		consumer = new Thread(runnable);
	}
	
	public void start() {
		this.consumer.start();
	}
	
	public boolean submitTask(WorkerTask task) {
		return taskQueue.add(task);
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
					wt = taskQueue.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				// Create worker to handle the task and submit to the pool
				Worker w = new Worker(wt, pc);
				Future<WorkerTaskResultFuture> future = pool.submit(w);
				
				// Store future
				resultQueue.add(future);
				
				// Check if there are futures that have finished at this point
				for(Future<WorkerTaskResultFuture> f : resultQueue) {
					if(f.isDone()) {
						// TODO: this one is done
					}
					else if(f.isCancelled()) {
						
					}
				}
			}
		}
	}

}
