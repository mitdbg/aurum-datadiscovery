package masterworker;

import java.util.ArrayList;
import java.util.List;

public class WorkerStatus {
	private boolean isRunning;
	private List<Integer> outstandingTasks;
	

	public WorkerStatus(boolean running) {
		this.isRunning = running;
		this.outstandingTasks = new ArrayList<Integer>();
	}
	
	public boolean isRunning() {
		return isRunning;
	}
	
	public void updateStatus(boolean running) {
		this.isRunning = running;
	}
	
	public List<Integer> getOutstandingTasks() {
		return this.outstandingTasks;
	}

	public void addTask(int taskId) {
		outstandingTasks.add(taskId);
	}

	public void completeTask(int taskId) {
		outstandingTasks.remove(taskId);
	}
}
