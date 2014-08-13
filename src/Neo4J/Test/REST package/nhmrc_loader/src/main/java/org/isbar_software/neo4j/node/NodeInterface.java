package org.isbar_software.neo4j.node;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.isbar_software.neo4j.Database;
import org.isbar_software.neo4j.utils.Neo4JException;
import org.isbar_software.neo4j.utils.Neo4JInterface;
import org.isbar_software.neo4j.utils.Neo4JOperation;
import org.isbar_software.neo4j.utils.OperationTypes;

import com.sun.jersey.api.client.ClientResponse;

public class NodeInterface extends Neo4JInterface {

	public NodeInterface(Database db) {
		super(db);		
	}

	@Override
	public String getUri() {
		return db.getServerUri() + "node";
	}

	private Neo4JOperation createNodeOperation(Map<String, Object> parameters) 
			throws JsonGenerationException, JsonMappingException, IOException {
		if (null == parameters || parameters.isEmpty())
			return new Neo4JOperation(
					OperationTypes.POST, 
					getUri());
		else
			return new Neo4JOperation(
					OperationTypes.POST, 
					getUri(),
					mapper.writeValueAsString(parameters));
	}
	
	public Node createNode() 
    		throws JsonParseException, JsonMappingException, IOException, Neo4JException 
	{
		return createNodeWithParameters(null);
	}
	
	public Node createNodeWithParameters(final Map<String, Object> parameters) 
    		throws JsonParseException, JsonMappingException, IOException, Neo4JException 
    {
    	ClientResponse response = Invoke(createNodeOperation(parameters));
    	
    	final String entitity = response.getEntity( String.class );
    	int status = response.getStatus();
    	response.close();
    	
    	if (status == 201)
    	{
    		Node node = mapper.readValue(entitity, Node.class);
    		node.setDatabase(db);
    		
    		return node;
    	}
    	else
    		throw mapper.readValue(entitity, Neo4JException.class); 
    } 
	
	public Node createNodeWithLabelAndParameters(final String label, final Map<String, Object> parameters) 
			throws JsonGenerationException, JsonMappingException, IOException {
		Map<String, Object> prop = new HashMap<String, Object>();
		prop.put("label", label);
		prop.put("prop", parameters);
		
		Map<String, Object> response = db.query("CREATE (n:" + label + " {prop}) RETURN n", prop);
		if (null != response) {
			List<Object> data = (List<Object>) response.get("data");
			if (null != data && !data.isEmpty()) {
				data = (List<Object>) data.get(0);
				if (null != data && !data.isEmpty()) {
					Map<String, Object> nodeMap = (Map<String, Object>) data.get(0);
					
					Node node = new Node();
					node.setDatabase(db);
					node.setUri((String) nodeMap.get("self"));
					node.setProperties((LinkedHashMap<String, Object>) nodeMap.get(data));
					
					return node;
				}
			}
		}
		
		return null;
	}
		
}
