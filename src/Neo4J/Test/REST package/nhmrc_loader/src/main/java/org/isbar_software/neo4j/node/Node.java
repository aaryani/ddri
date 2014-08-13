package org.isbar_software.neo4j.node;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.JsonMappingException;
import org.isbar_software.neo4j.utils.Neo4JObject;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Node extends Neo4JObject {
	private HashMap<String, Object> properties;
	
	@JsonProperty("self")
	public void setUri(String url) {
		super.setUri(url);
	}
	
	@JsonProperty("data")
	public Map<String, Object> getProperties() {
		return properties;
	}
	
	@JsonProperty("data")
	public void setProperties(HashMap<String, Object> properties) {
		this.properties = properties;
	}
	
	public Object getProperty(String key) {
		if (null == properties)
			return null;
		
		return properties.get(key);
	}
	
	public void setProperty(String key, Object value) {
		if (null == properties)
			properties = new HashMap<String, Object>();
		properties.put(key, value);
	}
	
	@JsonIgnore() 
	public void setLabels(final String ... labels) {
		try {
			db.labels().addNodeLabel(getUri(), labels);
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
