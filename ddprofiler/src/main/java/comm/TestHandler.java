package comm;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import java.util.Scanner;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import core.Conductor;
import core.TaskPackage;

public class TestHandler extends HttpServlet {	
	private static final long serialVersionUID = 1L;
	final private static Logger LOG = LoggerFactory.getLogger(WebHandler.class);

	private ObjectMapper om = new ObjectMapper();
	private Conductor c;

	public TestHandler(Conductor c) { this.c = c; }

	public void doPost(HttpServletRequest request, HttpServletResponse response) 
			throws ServletException, IOException {
		System.out.println("do get getting called");
		Map<String, String[]> parameters = request.getParameterMap();

		System.out.println("paramereters: "+ parameters.toString());
		readDirectoryAndCreateTasks(parameters.get("dbName")[0], c, parameters.get("pathToSources")[0], parameters.get("separator")[0]);


	}

	public void doGet(HttpServletRequest request, HttpServletResponse response) 
			throws ServletException, IOException {
		System.out.println("do post getting called");

		Map<String, String[]> parameters = request.getParameterMap();

		System.out.println("paramereters: "+ parameters.toString());

		response.setContentType("text/html");

		PrintWriter out = response.getWriter();

		out.println("<h1> Praise God <h1>");
	}

	private void readDirectoryAndCreateTasks(String dbName, Conductor c, String pathToSources,
			String separator) {
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
		LOG.info("Total files submitted for processing: {} - {}", totalFiles, tt);
	}


}
