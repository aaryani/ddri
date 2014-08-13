package org.isbar_software.neo4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.isbar_software.neo4j.node.NodeInterface;
import org.isbar_software.neo4j.node.label.LabelInterface;
import org.isbar_software.neo4j.schema.constrain.ConstraintInterface;
import org.isbar_software.neo4j.schema.index.IndexInterface;
import org.isbar_software.neo4j.utils.Neo4JInterface;
import org.isbar_software.neo4j.utils.Neo4JOperation;
import org.isbar_software.neo4j.utils.OperationTypes;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource.Builder;

public class Database {
	//private static final String SERVER_ROOT_URI = "http://54.83.73.225:7474/db/data/";
    private static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";
    
    private String serverUri;
    
    private IndexInterface indexInterface = null;
    private ConstraintInterface constraintInterface = null;
    private NodeInterface nodeInterface = null;
    private LabelInterface labelInterface = null;
    
    public Database() {
    	serverUri = SERVER_ROOT_URI;
    }
    
    public Database(String serverUri) {
    	setServerUri(serverUri);
    }
    
    public void setServerUri(String serverUri) {
    	if (null != serverUri)
    	{
    		this.serverUri = serverUri;
    		
    		// add '/' to the end
    		if (this.serverUri.charAt(this.serverUri.length()) != '/')
    			this.serverUri += '/';
    	}
    	else
    		this.serverUri = SERVER_ROOT_URI;
    }
    
    public String getServerUri() {
    	return serverUri;
    }
   
    public boolean checkServerStatus() {
        ClientResponse response = Neo4JInterface.Invoke( 
        		new Neo4JOperation(OperationTypes.GET, serverUri) );

        int status = response.getStatus();
        response.close();
        
        return status == 200;
    }
    
    public Map<String, Object> query(final String query) 
    		throws JsonGenerationException, JsonMappingException, IOException {
    	return query(query, null);
    }    
    
    public Map<String, Object> query(final String query, final Map<String, Object> parameters) 
    		throws JsonGenerationException, JsonMappingException, IOException {
    	Map<String, Object> par = new HashMap<String, Object>();
    	par.put("query", query);
    	par.put("params", parameters);
    	
    	ClientResponse response = Neo4JInterface.Invoke( 
    			new Neo4JOperation(OperationTypes.POST, serverUri + "cypher", 
    					Neo4JInterface.toJson(par)) );
    	
    	int status = response.getStatus();
    	final String entity = response.getEntity( String.class );
    	
    	response.close();
    	
    	Map<String, Object> result = Neo4JInterface.fromJson(entity);     
     	if (status == 200) {
     		
         
     		return result;     		
     	}
     	else
     		return null;
    }
     
    
    public IndexInterface indexes() {
    	return null == indexInterface ? (indexInterface = new IndexInterface(this)) : indexInterface;
    }
    
    public ConstraintInterface constrains() {
    	return null == constraintInterface ? (constraintInterface = new ConstraintInterface(this)) : constraintInterface;
    }
    
    public NodeInterface nodes() {
    	return null == nodeInterface ? (nodeInterface = new NodeInterface(this)) : nodeInterface;
    }
    
    public LabelInterface labels() {
    	return null == labelInterface ? (labelInterface = new LabelInterface(this)) : labelInterface;
    }
    
}
