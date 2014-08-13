package org.isbar_software.neo4j.utils;

import org.isbar_software.neo4j.Database;

public abstract class Neo4JObject {
	protected String uri;
	protected Database db;
	
	public Database getDatabase() {
		return db;
	}
	
	public void setDatabase(Database db) {
		this.db = db;
		
		updateUri();
	}

	public String getUri() {
		return uri;
	}
	
	public void setUri(String uri) {
		this.uri = uri;
	}
	
	protected void updateUri() {}
	
}
