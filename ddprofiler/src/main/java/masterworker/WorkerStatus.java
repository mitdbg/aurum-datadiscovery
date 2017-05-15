package masterworker;

public class WorkerStatus {
	private boolean isRunning;
	private String currentTask;
	
	public WorkerStatus(boolean running) {
		this.isRunning = running;
	}
	
	public boolean isRunning() {
		return isRunning;
	}
	
	public void updateStatus(boolean running) {
		this.isRunning = running;
	}
	
	public String getCurrentTask() {
		return this.currentTask;
	}
}
