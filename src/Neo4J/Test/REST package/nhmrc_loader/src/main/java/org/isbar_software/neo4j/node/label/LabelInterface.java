package org.isbar_software.neo4j.node.label;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.isbar_software.neo4j.Database;
import org.isbar_software.neo4j.node.Node;
import org.isbar_software.neo4j.utils.Neo4JInterface;
import org.isbar_software.neo4j.utils.Neo4JOperation;
import org.isbar_software.neo4j.utils.OperationTypes;

import com.sun.jersey.api.client.ClientResponse;

public class LabelInterface extends Neo4JInterface {

	public LabelInterface(Database db) {
		super(db);
	}

	@Override
	public String getUri() {
		return db.getServerUri() + "labels";
	}
	
	public String getNodeUri(final String nodeUri) {
		return nodeUri + "/labels";
	}
	
	private Neo4JOperation addNodeLabelOperation(final String nodeUri, final String[] labels) 
			throws JsonGenerationException, JsonMappingException, IOException {
		if (labels.length == 1)
			return new Neo4JOperation(OperationTypes.POST, 
					getNodeUri(nodeUri),
					mapper.writeValueAsString(labels[0]));
		else
			return new Neo4JOperation(OperationTypes.POST, 
					getNodeUri(nodeUri),
					mapper.writeValueAsString(labels));
	}
	
	public Boolean addNodeLabel(final Node node, final String ... labels) 
			throws JsonGenerationException, JsonMappingException, IOException {
		return addNodeLabel(node.getUri(), labels);
	}
	
	public Boolean addNodeLabel(final String nodeUri, final String ... labels) 
			throws JsonGenerationException, JsonMappingException, IOException 
    {
    	ClientResponse response = Invoke(addNodeLabelOperation(nodeUri, labels));
    	int status = response.getStatus();
    	
    	return status == 204; // no content
    } 
	
	private Neo4JOperation replaceNodeLabelOperation(final String nodeUri, final String[] labels) 
			throws JsonGenerationException, JsonMappingException, IOException {
		if (labels.length == 1)
			return new Neo4JOperation(OperationTypes.PUT, 
					getNodeUri(nodeUri),
					mapper.writeValueAsString(labels[0]));
		else
			return new Neo4JOperation(OperationTypes.PUT, 
					getNodeUri(nodeUri),
					mapper.writeValueAsString(labels));
	}
	
	public Boolean replaceNodeLabel(final Node node, final String ... labels) 
			throws JsonGenerationException, JsonMappingException, IOException {
		return replaceNodeLabel(node.getUri(), labels);
	}
	
	public Boolean replaceNodeLabel(final String nodeUri, final String ... labels) 
			throws JsonGenerationException, JsonMappingException, IOException 
    {
    	ClientResponse response = Invoke(replaceNodeLabelOperation(nodeUri, labels));
    	int status = response.getStatus();
    	
    	return status == 204; // no content
    } 

}
