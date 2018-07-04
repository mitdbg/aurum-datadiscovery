/**
 *
 */
/**
 * @author Sibo Wang
 *
 */

package sources.deprecated;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Config {
  private static final Logger log = Logger.getLogger(Config.class.getName());
  public String getConn_type() { return conn_type; }
  public void setConn_type(String conn_type) { this.conn_type = conn_type; }
  public String getConn_path() { return conn_path; }
  public void setConn_path(String conn_path) { this.conn_path = conn_path; }
  public String getConn_filename() { return conn_filename; }
  public void setConn_filename(String conn_filename) {
    this.conn_filename = conn_filename;
  }
  public String getConn_ip() { return conn_ip; }
  public void setConn_ip(String conn_ip) { this.conn_ip = conn_ip; }
  public String getPort() { return port; }
  public void setPort(String port) { this.port = port; }

  public String getUser_name() { return user_name; }
  public void setUser_name(String user_name) { this.user_name = user_name; }
  public String getPassword() { return password; }
  public void setPassword(String password) { this.password = password; }
  public String getDb_system_name() { return db_system_name; }
  public void setDb_system_name(String db_system_name) {
    this.db_system_name = db_system_name;
  }

  private String db_system_name; // db system name e.g., mysq/oracle etc.
  private String user_name;      // db conn user name;
  private String password;       // db conn password;
  private String conn_type;      // whether it is file connector or db connector
  private String conn_path; // for file dir (csv) or database dir (in database)
  private String conn_filename; // for file name (csv) or table (in database)
  private String conn_ip;       // for database
  private String port;          // db connection port
  private String spliter;

  public void load_config_file(String config_file) throws IOException {
    Properties prop = new Properties();
    InputStream input = null;
    input = new FileInputStream(config_file);
    prop.load(input);

    conn_type = prop.getProperty("conn_type");
    if (conn_type.equals("FILE")) {
      conn_path = prop.getProperty("conn_path");
      conn_filename = prop.getProperty("conn_filename");
      System.out.println(prop.getProperty("spliter"));
      spliter = prop.getProperty("spliter");
      spliter = spliter.substring(1, spliter.length() - 1);
      // System.out.println("spliter:"+spliter);
    } else if (conn_type.equals("DB")) {
      conn_ip = prop.getProperty("conn_ip");
      conn_path = prop.getProperty("conn_path");
      conn_filename = prop.getProperty("conn_filename");
      db_system_name = prop.getProperty("db_system_name");
      port = prop.getProperty("port");
      user_name = prop.getProperty("user_name");
      password = prop.getProperty("password");
    } else {
      log.log(Level.SEVERE, "Incorrect configuration file with configure type",
              conn_type);
      throw new IOException();
    }
  }
  public String getSpliter() { return spliter; }
  public void setSpliter(String spliter) { this.spliter = spliter; }
}
