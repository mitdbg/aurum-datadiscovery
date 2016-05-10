/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package comm;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;

import core.config.ProfilerConfig;

public class WebServer {

	private Server server;
	private ServletContextHandler context;
	private ServletHolder jerseyServlet;
	
	public WebServer(ProfilerConfig pc) {
		context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
 
        server = new Server(pc.getInt(ProfilerConfig.WEB_SERVER_PORT));
        server.setHandler(context);
 
        jerseyServlet = context.addServlet(ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(0);
 
        // Tells the Jersey Servlet which REST service/class to load.
        jerseyServlet.setInitParameter("jersey.config.server.provider.classnames", WebHandler.class.getCanonicalName());
 
	}
	
	public void init() {
		try {
            server.start();
            server.join();
        } 
		catch (Exception e) {
			// TODO Handle this properly
			e.printStackTrace();
		} 
		finally {
            server.destroy();
        }
	}
	
	public void close() {
		try {
			server.stop();
		} 
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
