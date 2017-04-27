package masterworker;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

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

	private boolean pendingWork;

	public Worker(ProfilerConfig pc, Conductor c) {
		this.pc = pc;
		this.c = c;

		masterAddr = Integer.toString(pc.getInt(ProfilerConfig.MASTER_SERVER_PORT));
		addr = Integer.toString(pc.getInt(ProfilerConfig.WORKER_SERVER_PORT));
	}

	public void start() {
		System.out.println("STARTING WORKER");
		pendingWork = true;

		Worker worker = this;

		Thread server = new Thread() {// Not sure if I should be doing it like
										// this, but ws.init was hanging
									// something to do with how WebServer.init() is setup
			public void run() {
				WebServer ws = new WebServer(pc, c, worker);
				ws.init();
			}
		};

		server.start();

		System.out.println("get here");
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
				//System.out.println("TRYING to get response from server at: " + httpget.getURI());
				//System.out.println("reqeust I'm sending: " + httpget);
				HttpResponse response = (HttpResponse) httpclient.execute(httpget);// try to connect to worker
				System.out.println("response: " + response);
				break;
			} catch (URISyntaxException | IOException e) {
				//e.printStackTrace();
			}
		}
			// check for all done
		while (pendingWork) {
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
					// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("WOrker done");

	}

	public String processTask(TaskPackage task) {
		c.submitTask(task);
		return "OK";
	}
	
	public String stop() {
		System.out.println("stopping worker");
		c.stop();
	    pendingWork = false;
	    
	    return "OK";
	}

	private void readDirectoryAndCreateTasks(String dbName, Conductor c, String pathToSources, String separator) {
		File folder = new File(pathToSources);
		File[] filePaths = folder.listFiles();
		int totalFiles = 0;
		int tt = 0;
		for (File f : filePaths) {
			tt++;
			if (f.isFile()) {
				String path = f.getParent() + File.separator;
				String name = f.getName();
				TaskPackage tp = TaskPackage.makeCSVFileTaskPackage(dbName, path, name, separator);
				totalFiles++;
				c.submitTask(tp);
			}
		}
		System.out.printf("Total files submitted for processing: %d - %d", totalFiles, tt);
		
		 while (c.isTherePendingWork()) {
		      try {
		        Thread.sleep(3000);
		      } catch (InterruptedException e) {
		        // TODO Auto-generated catch block
		        e.printStackTrace();
		      }
		    }
		 System.out.println("this one");
		 
		 notifyMasterDone(pathToSources);
	}

	private void notifyMasterDone(String taskName) {
		HttpClient httpclient = HttpClients.createDefault();

		URIBuilder builder = new URIBuilder();
		builder.setScheme("http")
		.setHost("localhost:" + masterAddr)
		.setPath("/dd")
		.setParameter("actionid","taskComplete")
		.setParameter("workerAddr", addr)
		.setParameter("taskName", taskName);

		while (true) {

			HttpGet httpget;
			try {
				httpget = new HttpGet(builder.build());
				//System.out.println("TRYING to get response from server at: " + httpget.getURI());
				//System.out.println("reqeust I'm sending: " + httpget);
				HttpResponse response = (HttpResponse) httpclient.execute(httpget);// try to connect to worker
				System.out.println("response: " + response);
				break;
			} catch (URISyntaxException | IOException e) {
				//e.printStackTrace();
			}
		}
	}
}
