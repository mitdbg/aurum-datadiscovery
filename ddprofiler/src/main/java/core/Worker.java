package core;

import java.util.concurrent.Callable;

public class Worker implements Callable<WorkerTaskResult> {

	private WorkerTask task;
	
	public Worker(WorkerTask task) {
		this.task = task;
	}

	@Override
	public WorkerTaskResult call() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
