package sources.connectors;

import static com.codahale.metrics.MetricRegistry.name;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.codahale.metrics.Counter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import core.Conductor;
import metrics.Metrics;
import sources.config.SQLServerSourceConfig;
import sources.main.SourceType;

public class SQLServerConnector implements Connector {

    private SQLServerSourceConfig config;

    private Connection connection;

    private TableInfo tableInfo;
    private boolean firstTime = true;
    private Statement theStatement;
    private ResultSet theRS;

    // Metrics
    private Counter error_records = Metrics.REG.counter((name(PostgresConnector.class, "error", "records")));
    private Counter success_records = Metrics.REG.counter((name(PostgresConnector.class, "success", "records")));

    public SQLServerConnector(SQLServerSourceConfig config) {
	this.config = config;

	this.tableInfo = new TableInfo();

	try {
	    this.initConnector();
	} catch (ClassNotFoundException | IOException | SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	// Initialize tbInfo
	List<Attribute> attrs = null;
	try {
	    attrs = this.getAttributes();
	} catch (SQLException | IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	this.tableInfo.setTableAttributes(attrs);
    }

    @Override
    public SourceType getSourceType() {
	return SourceType.sqlserver;
    }

    @Override
    public void initConnector() throws IOException, ClassNotFoundException, SQLException {
	// Definition of a conn identifier is here
	String ip = config.getDb_server_ip();
	String port = new Integer(config.getDb_server_port()).toString();
	String connPath = config.getDatabase_name();
	String username = config.getDb_username();
	String password = config.getDb_password();
	String dbName = config.getDatabase_name();

	String connIdentifier = config.getDatabase_name() + ip + port;

	if (Conductor.connectionPools.containsKey(connIdentifier)) {
	    this.connection = Conductor.connectionPools.get(connIdentifier);
	    return;
	}

	Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
	String cPath = "jdbc:sqlserver://"+ip+":"+port+"; databaseName="+dbName;

	// If no existing pool to handle this db, then we create a new one
	HikariConfig config = new HikariConfig();
	config.setJdbcUrl(cPath);
	config.setUsername(username);
	config.setPassword(password);
	config.addDataSourceProperty("cachePrepStmts", "true");
	config.addDataSourceProperty("prepStmtCacheSize", "250");
	config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
	config.addDataSourceProperty("maximumPoolSize", "1");
	HikariDataSource ds = new HikariDataSource(config);

	Connection connection = null;
	try {
	    connection = ds.getConnection();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	Conductor.connectionPools.put(connIdentifier, connection);

	this.connection = connection;
    }

    @Override
    public void destroyConnector() {
	try {
	    // this.connection.close();
	    this.theRS.close();
	    this.theStatement.close();
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    @Override
    public List<Attribute> getAttributes() throws IOException, SQLException {
	if (tableInfo.getTableAttributes() != null)
	    return tableInfo.getTableAttributes();
	DatabaseMetaData metadata = connection.getMetaData();
	ResultSet resultSet = metadata.getColumns(null, null, config.getRelationName(), null);
	Vector<Attribute> attrs = new Vector<Attribute>();
	while (resultSet.next()) {
	    String name = resultSet.getString("COLUMN_NAME");
	    String type = resultSet.getString("TYPE_NAME");
	    int size = resultSet.getInt("COLUMN_SIZE");
	    Attribute attr = new Attribute(name, type, size);
	    attrs.addElement(attr);
	}
	resultSet.close();
	tableInfo.setTableAttributes(attrs);
	return attrs;
    }

    @Override
    public Map<Attribute, List<String>> readRows(int num) throws IOException, SQLException {
	Map<Attribute, List<String>> data = new LinkedHashMap<>();
	// Make sure attrs is populated, if not, populate it here
	if (data.isEmpty()) {
	    List<Attribute> attrs = this.getAttributes();
	    attrs.forEach(a -> data.put(a, new ArrayList<>()));
	}

	// Read data and insert in order
	List<Record> recs = new ArrayList<>();
	boolean readData = this.read(num, recs);
	if (!readData) {
	    return null;
	}

	for (Record r : recs) {
	    List<String> values = r.getTuples();
	    int currentIdx = 0;
	    if (values.size() != data.values().size()) {
		error_records.inc();
		continue; // Some error while parsing data, a row has a
			  // different format
	    }
	    success_records.inc();
	    for (List<String> vals : data.values()) { // ordered iteration
		vals.add(values.get(currentIdx));
		currentIdx++;
	    }
	}
	return data;
    }

    private boolean read(int num, List<Record> rec_list) throws SQLException {
	if (firstTime) {
	    handleFirstTime(num);
	    firstTime = false;
	}

	boolean new_row = false;

	while (num > 0 && theRS.next()) { // while there are some available and
					  // we need to read more records
	    new_row = true;

	    num--;
	    // FIXME: profile and optimize this
	    Record rec = new Record();
	    for (int i = 0; i < this.tableInfo.getTableAttributes().size(); i++) {
		Object obj = theRS.getObject(i + 1);
		if (obj != null) {
		    String v1 = obj.toString();
		    rec.getTuples().add(v1);
		} else {
		    rec.getTuples().add("");
		}
	    }
	    rec_list.add(rec);
	}

	return new_row;
    }

    private boolean handleFirstTime(int fetchSize) {
	String sql = "SELECT * FROM " + config.getRelationName();

	try {
	    connection.setAutoCommit(false);
	    theStatement = connection.createStatement();
	    theStatement.setFetchSize(fetchSize);
	    theRS = theStatement.executeQuery(sql);
	} catch (SQLException sqle) {
	    System.out.println("ERROR: " + sqle.getLocalizedMessage());
	    return false;
	} catch (Exception e) {
	    System.out.println("ERROR: executeQuery failed");
	    return false;
	}
	return true;
    }

}
