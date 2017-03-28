package masterworker;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;

import comm.WebServer;
import core.Conductor;
import core.config.ProfilerConfig;

public class Master {

	private ProfilerConfig pc;
	private Conductor c;
	private String dbName;

	private boolean pendingWork;
	// workers
	private HashMap<String, WorkerStatus> workers;
	
	//tasks
	private Map<String, Boolean> tasks;
	
	// catalog
	// private Map<String, String> catalog = new HashMap<String, String>();

	public Master(ProfilerConfig pc, Conductor c) {
		this.pc = pc;
		this.c = c;
		this.workers = new HashMap<String, WorkerStatus>();
		this.dbName = pc.getString(ProfilerConfig.DB_NAME);
	}

	public void start(String sourcePath) {
		pendingWork = true;
		// split up work
		tasks = new HashMap<String, Boolean>();
		tasks.put(sourcePath, true);
		
		System.out.println("tasks: " + tasks.toString());
		// check catalog for tasks already complete
		
		Master master = this;

		Thread server = new Thread() {// Not sure if I should be doing it like
										// this, but ws.init was hanging, I think it has 
										// something to do with how WebServer.init() is setup
			public void run() {
				WebServer ws = new WebServer(pc, c, master);
				ws.init();
			}
		};

		server.start();

		while (pendingWork) {
			try {
				System.out.println("pending work: " + pendingWork);
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		stopWorkers();
	}

	private void stopWorkers() {
		for (String workerAddr : workers.keySet()) {
				//if (workers.get(workerAddr).isRunning())  check worker status
			stopWorker(workerAddr);
		}
	}

	private void stopWorker(String workerAddr) {

		HttpClient httpclient = HttpClients.createDefault();

		URIBuilder builder = new URIBuilder();
		builder.setScheme("http")
		.setHost("localhost:" + workerAddr)
		.setPath("/dd")
		.setParameter("actionid", "stopWorker");

		// Execute and get the response.
		HttpGet httpget;
		try {
			httpget = new HttpGet(builder.build());
				//System.out.println("TRYING to get response from server at: " + httpget.getURI());
				//System.out.println("reqeust I'm sending: " + httpget);
				Object response = httpclient.execute(httpget);// try to connect to worker
				System.out.println("response for stop task: " + response);
			} catch (URISyntaxException | IOException e) {
				e.printStackTrace();
			}
	}

	public String registerWorker(String workerAddr) {
			WorkerStatus newWorkerStatus = new WorkerStatus(false);
			workers.put(workerAddr, newWorkerStatus);

			System.out.println("workders: " + workers.toString());

		// give worker chunk of work to process
			for (String task : tasks.keySet()) {
			if (tasks.get(task) != null && tasks.get(task)) {// check if this task still needs to be done
				
				processPathOnWorker(task, workerAddr);
				return "OK";
			}
		}
		
		// no more work to do
		pendingWork = false;
		return "OK";
	}
	
	public String taskComplete(String workerAddr, String taskName) {
		// update worker status
		workers.get(workerAddr).updateStatus(false);
		
		// mark task complete
		tasks.put(taskName, false);
		
		for (String task : tasks.keySet()) {
			if (tasks.get(task)) {// check if this task still needs to be done
				
				processPathOnWorker(task, workerAddr);
				return "OK";
			}
		}
		System.out.println("master finished everything");
		// no more work to do
		pendingWork = false;
		return "OK";
	}

	private void processPathOnWorker(String taskPath, String workerAddr) {

		HttpClient httpclient = HttpClients.createDefault();

		URIBuilder builder = new URIBuilder();
		builder.setScheme("http")
		.setHost("localhost:" + workerAddr)
		.setPath("/dd")
		.setParameter("actionid", "processTaskOnWorker")
		.setParameter("dbName", dbName)
		.setParameter("source", taskPath);

		// Execute and get the response.
		HttpGet httpget;
		try {
			httpget = new HttpGet(builder.build());
				//System.out.println("TRYING to get response from server at: " + httpget.getURI());
				//System.out.println("reqeust I'm sending: " + httpget);
				Object response = httpclient.execute(httpget);// try to connect to worker
				System.out.println("response for process task: " + response);
			} catch (URISyntaxException | IOException e) {
				e.printStackTrace();
			}
		}
	}
