/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package core.config;

import java.util.List;
import java.util.Map;

import core.config.ConfigDef.Importance;
import core.config.ConfigDef.Type;

public class ProfilerConfig extends Config {

  private static final ConfigDef config;

  public static final String DB_NAME = "db.name";
  public static final String DB_NAME_DOC = "Name used for the data repository";
  
  public static final String EXECUTION_MODE = "execution.mode";
  private static final String EXECUTION_MODE_DOC =
      "(online - 0) for server mode and (offline files - 1) for one-shot read files from directory and (offline db - 2) for one-shot read tables from db, (benchmark 3) for benchmarking purposes";

  public static final String WEB_SERVER_PORT = "web.server.port";
  private static final String WEB_SERVER_PORT_DOC =
      "The port where web server listens";

  public static final String NUM_POOL_THREADS = "num.pool.threads";
  private static final String NUM_POOL_THREADS_DOC =
      "Number of threads in the worker pool";

  public static final String NUM_RECORD_READ = "num.record.read";
  private static final String NUM_RECORD_READ_DOC =
      "Number of records to read per interaction with the data sources";

  public static final String STORE_TYPE = "store.type";
  private static final String STORE_TYPE_DOC =
      "Configures store type: NULL(0), ELASTIC_HTTP(1), ELASTIC_NATIVE(2)";

  public static final String STORE_HTTP_PORT = "store.http.port";
  private static final String STORE_HTTP_PORT_DOC =
      "Server HTTP port for stores that support it";

  public static final String STORE_SERVER = "store.server";
  private static final String STORE_SERVER_DOC =
      "Server name or IP where the store lives";

  public static final String STORE_PORT = "store.port";
  private static final String STORE_PORT_DOC = "Port where the store listens";

  public static final String SOURCES_TO_ANALYZE_FOLDER = "sources.folder.path";
  private static final String SOURCES_TO_ANALYZE_FOLDER_DOC =
      "Path to a folder with files to analyze";

  public static final String SOURCES_TO_ANALYZE_DB = "sources.db.path";
  private static final String SOURCES_TO_ANALYZE_DB_DOC =
      "Path to db with tables to analyze";

  public static final String ERROR_LOG_FILE_NAME = "error.logfile.name";
  private static final String ERROR_LOG_FILE_NAME_DOC =
      "Name of log file that records the errors while profiling data";

  public static final String CSV_SEPARATOR = "csv.separator";
  private static final String CSV_SEPARATOR_DOC =
      "The separator used to split CSV/TSV files";
  
  public static final String REPORT_METRICS_CONSOLE = "console.metrics";
  private static final String REPORT_METRICS_CONSOLE_DOC = "Output metrics to console";

  static {
    config =
        new ConfigDef()
        	.define(DB_NAME, Type.STRING, "", Importance.HIGH, DB_NAME_DOC)
            .define(EXECUTION_MODE, Type.INT, 0, Importance.HIGH,
                    EXECUTION_MODE_DOC)
            .define(WEB_SERVER_PORT, Type.INT, 8080, Importance.MEDIUM,
                    WEB_SERVER_PORT_DOC)
            .define(NUM_POOL_THREADS, Type.INT, 4, Importance.LOW,
                    NUM_POOL_THREADS_DOC)
            .define(NUM_RECORD_READ, Type.INT, 10000, Importance.MEDIUM,
                    NUM_RECORD_READ_DOC)
            .define(STORE_TYPE, Type.INT, 2, Importance.MEDIUM, STORE_TYPE_DOC)
            .define(STORE_SERVER, Type.STRING, "127.0.0.1", Importance.HIGH,
                    STORE_SERVER_DOC)
            .define(STORE_HTTP_PORT, Type.INT, 9200, Importance.HIGH,
                    STORE_HTTP_PORT_DOC)
            .define(STORE_PORT, Type.INT, 9300, Importance.HIGH, STORE_PORT_DOC)
            .define(SOURCES_TO_ANALYZE_FOLDER, Type.STRING, ".", Importance.LOW,
                    SOURCES_TO_ANALYZE_FOLDER_DOC)
            .define(SOURCES_TO_ANALYZE_DB, Type.STRING, ".", Importance.LOW,
                    SOURCES_TO_ANALYZE_DB_DOC)
            .define(ERROR_LOG_FILE_NAME, Type.STRING, "error_profiler.log",
                    Importance.MEDIUM, ERROR_LOG_FILE_NAME_DOC)
            .define(CSV_SEPARATOR, Type.STRING, ",", Importance.MEDIUM,
                    CSV_SEPARATOR_DOC)
    		.define(REPORT_METRICS_CONSOLE, Type.INT, -1, 
    				Importance.HIGH, REPORT_METRICS_CONSOLE_DOC);
  }

  public ProfilerConfig(Map<? extends Object, ? extends Object> originals) {
    super(config, originals);
  }

  public static ConfigKey getConfigKey(String name) {
    return config.getConfigKey(name);
  }

  public static List<ConfigKey> getAllConfigKey() {
    return config.getAllConfigKey();
  }

  public static void main(String[] args) {
    System.out.println(config.toHtmlTable());
  }
}
