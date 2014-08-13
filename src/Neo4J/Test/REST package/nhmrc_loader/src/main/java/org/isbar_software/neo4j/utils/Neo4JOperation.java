package org.isbar_software.neo4j.utils;

public class Neo4JOperation {
	protected OperationTypes operation;
	protected String uri;
	protected String json;
	
	public Neo4JOperation(OperationTypes operation, String uri) {
	/*	if (operation != OperationTypes.GET && operation != OperationTypes.DELETE)
			throw new InvalidRestOperationException();*/
		
		this.operation = operation;
		this.uri = uri;
	}
	
	public Neo4JOperation(OperationTypes operation, String uri, String json) {
		/*if (operation != OperationTypes.PUT && operation != OperationTypes.POST)
			throw new InvalidRestOperationException();*/
		
		this.operation = operation;
		this.uri = uri;
		this.json = json;
	}
	

	public OperationTypes getOperation() {
		return operation;
	}
	
	public String getUri() {
		return uri;
	}
	
	public String getJson() {
		return json;
	}	
}
