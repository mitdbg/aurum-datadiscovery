package core.config.sources;

import com.fasterxml.jackson.annotation.JsonProperty;

import core.SourceType;

public class PostgresSourceConfig implements SourceConfig {

    private String sourceName;

    @JsonProperty
    private String db_server_ip;

    @JsonProperty
    private int db_server_port;

    @JsonProperty
    private String database_name;

    @JsonProperty
    private String db_username;

    @JsonProperty
    private String db_password;

    public String getDb_server_ip() {
	return db_server_ip;
    }

    public int getDb_server_port() {
	return db_server_port;
    }

    public String getDatabase_name() {
	return database_name;
    }

    public String getDb_username() {
	return db_username;
    }

    public String getDb_password() {
	return db_password;
    }

    @Override
    public String getSourceName() {
	return sourceName;
    }

    @Override
    public void setSourceName(String sourceName) {
	this.sourceName = sourceName;
    }

    @Override
    public SourceType getSourceType() {
	return SourceType.postgres;
    }

}
