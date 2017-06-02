package masterworker;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;


import comm.WebServer;
import core.Conductor;
import core.TaskPackage;
import core.config.ProfilerConfig;

public class Worker {

	private ProfilerConfig pc;
	private Conductor c;

	private String addr;
	private String masterAddr;
	private List<Integer> taskQueue;

	private boolean pendingWork;
	private ReentrantLock lock;

	public Worker(ProfilerConfig pc, Conductor c) {
		this.pc = pc;
		this.c = c;
		this.taskQueue = new ArrayList<Integer>();
		this.lock = new ReentrantLock();

		masterAddr = Integer.toString(pc.getInt(ProfilerConfig.MASTER_SERVER_PORT));
		addr = Integer.toString(pc.getInt(ProfilerConfig.WORKER_SERVER_PORT));
	}

	public void start() {
		pendingWork = true;

		Worker worker = this;

		Thread server = new Thread() {
			public void run() {
				WebServer ws = new WebServer(pc, c, worker);
				ws.init();
			}
		};

		server.start();

		// try to connect to leader
		HttpClient httpclient = HttpClients.createDefault();

		URIBuilder builder = new URIBuilder();
		builder.setScheme("http")
		.setHost("localhost:" + masterAddr)
		.setPath("/dd")
		.setParameter("actionid","registerWorker")
		.setParameter("workerAddr", addr);

		while (true) {

			HttpGet httpget;
			try {
				httpget = new HttpGet(builder.build());

				HttpResponse response = (HttpResponse) httpclient.execute(httpget);// try to connect to master
				break;
			} catch (URISyntaxException | IOException e) {
				//e.printStackTrace();
			}

			try {
				Thread.sleep(4000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// check for all done
		while (pendingWork) {			
			if (taskQueue.size() > 0 ) {
				if (!c.isTherePendingWork()) {
					// done with this batch
					getWork();
				}
			}
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public String processTask(TaskPackage task) {
		taskQueue.add(task.getId());
		c.submitTask(task);

		return "OK";
	}


	public String stop() {
		c.stop();
		pendingWork = false;

		return "OK";
	}

	private void getWork() {
		String taskIdStr = "";
		for (int taskId : taskQueue) {
			taskIdStr += Integer.toString(taskId) + " ";
		}
		
		taskQueue = new ArrayList<Integer>();		
		HttpClient httpclient = HttpClients.createDefault();

		URIBuilder builder = new URIBuilder();
		builder.setScheme("http")
		.setHost("localhost:" + masterAddr)
		.setPath("/dd")
		.setParameter("actionid","taskComplete")
		.setParameter("workerAddr", addr)
		.setParameter("taskIds", taskIdStr);

		while (true) {

			HttpGet httpget;
			try {
				httpget = new HttpGet(builder.build());
				HttpResponse response = (HttpResponse) httpclient.execute(httpget);// try to connect to worker
				break;
			} catch (URISyntaxException | IOException e) {
				e.printStackTrace();
			}
		}
	}
}
