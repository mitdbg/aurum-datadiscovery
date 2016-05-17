/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package comm;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path("/dd")
public class WebHandler {
	
	// Jackson (JSON) serializer
	private ObjectMapper om = new ObjectMapper();
    
    @GET
    @Path("init_store")
    public void initStore() {
    	// TODO: initialize connection to store
    }
    
    @GET
    @Path("new_data_source")
    @Produces(MediaType.TEXT_PLAIN)
    public String newDataSource() {
    	
    	return "";
    }
    
    @GET
    @Path("new_attribute")
    @Produces(MediaType.TEXT_PLAIN)
    public String newAttribute() {
    	
    	return "";
    }
    
    
    // Embedded Testing functions
    
    @GET
    @Path("test")
    @Produces(MediaType.TEXT_PLAIN)
    public String test() {
        return "Hello World";
    }
    
    @GET
    @Path("test2")
    @Produces(MediaType.TEXT_PLAIN)
    public String test2(@QueryParam("a") String a, @QueryParam("b") String b, @QueryParam("c") String c, @Context UriInfo uriInfo) {
		return a + "-" + b + "-" + c;
    }
    
    @GET
    @Path("test3")
    @Produces(MediaType.TEXT_PLAIN)
    public String test3(@QueryParam("a") String a, @QueryParam("b") String b) {
    	Test t = new Test(a, b);
    	String response = "";
    	try {
			response = om.writeValueAsString(t);
		} 
    	catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return response;
    }
    
    class Test {
		private String a;
		private String b;
		public Test(String a, String b) {
			this.a = a;
			this.b = b;
		}
		public String getA() {
			return a;
		}
		public void setA(String a) {
			this.a = a;
		}
		public String getB() {
			return b;
		}
		public void setB(String b) {
			this.b = b;
		}
	}
    
}
