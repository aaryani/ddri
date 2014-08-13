package org.isbar_software.neo4j.schema.constrain;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.isbar_software.neo4j.utils.Neo4JObject;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Constraint extends Neo4JObject{
	private String label;
	private String type;
	private String[] property_keys;
	
	@JsonProperty("label")
	public String getLabel() {
		return label;
	}
	
	@JsonProperty("label")
	public void setLabel(final String label) {
		this.label = label;
	}
	
	@JsonProperty("type")
	public String getType() {
		return type;
	}
	
	@JsonProperty("type")
	public void setType(final String type) {
		this.type = type;
	}
		
	@JsonProperty("property_keys")
	public String[] getPropertyKeys() {
		return property_keys;		
	}
		
	@JsonProperty("property_keys")
	public void setPropertyKeys(final String[] keys) {
		this.property_keys = keys;
	}

	@Override
	protected void updateUri() {
		uri = db.constrains().getUri() + "/" + label + "/uniqueness";		
	}		
}
