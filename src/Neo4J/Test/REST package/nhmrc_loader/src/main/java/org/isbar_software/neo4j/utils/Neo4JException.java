package org.isbar_software.neo4j.utils;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Neo4JException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2165798615510683588L;
	private String message;
	private String exception;
	
	@JsonProperty("message")
	public String getMessage() {
		return message;
	}
	
	@JsonProperty("message")
	public void setMessage(final String message) {
		this.message = message;
	}
	
	@JsonProperty("exception")
	public String getException() {
		return exception;
	}
	
	@JsonProperty("exception")
	public void setException(final String exception) {
		this.exception = exception;
	}
	
	@Override
    public String toString() {     
        StringBuffer theString = new StringBuffer();  
        
        theString.append("NEO4J Error.");
        if (message != null && !message.isEmpty()) {
        	theString.append(" Message: ");
        	theString.append(message);
        	theString.append('.');
        }
        if (exception != null && !exception.isEmpty()) {
        	theString.append(" Exception: ");
        	theString.append(exception);
        	theString.append('.');
        }
     
        return theString.toString();  
    }  
	
}
