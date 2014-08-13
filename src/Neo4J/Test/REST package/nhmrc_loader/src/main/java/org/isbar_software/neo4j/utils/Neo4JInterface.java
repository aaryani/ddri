package org.isbar_software.neo4j.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;
import org.codehaus.jackson.type.TypeReference;
import org.isbar_software.neo4j.Database;
import org.isbar_software.neo4j.schema.index.Index;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource.Builder;

public abstract class Neo4JInterface {
	
    protected static final String HEADER_X_STREAM = "X-Stream";

	public static final ObjectMapper mapper = new ObjectMapper();	
	public static final TypeReference<HashMap<String, Object>> hashMapTypeReference = new TypeReference<HashMap<String, Object>>() {};   
	
	protected Database db;	
	
	public Neo4JInterface(Database db) {
		this.db = db;
		
	}

	public abstract String getUri();
	
    public static ClientResponse Invoke( Neo4JOperation op ) {
    	Builder builder = Client.create()
    			.resource( op.getUri() )
    			.accept( MediaType.APPLICATION_JSON )
    			.header( HEADER_X_STREAM, "true" );
		
    	ClientResponse response = null;
    	
    	switch (op.getOperation())
    	{
    	case GET:
    		return builder.get( ClientResponse.class );
    		
    	case PUT:
        	if (op.getJson() != null)
    			builder = builder
    					.type( MediaType.APPLICATION_JSON )
    					.entity( op.getJson() );

        	return builder.put( ClientResponse.class );

    	case POST:
        	if (op.getJson() != null)
    			builder = builder
    					.type( MediaType.APPLICATION_JSON )
    					.entity( op.getJson() );

        	return builder.post( ClientResponse.class );
    		
    	case DELETE:
    		return builder.delete( ClientResponse.class );
    
    	default: // invalid operation?
    		return null;
    	}
    }    
    
    public static String toJson(Object o) 
    		throws JsonGenerationException, JsonMappingException, IOException {
    	return mapper.writeValueAsString(o);
    }
    
    public static <T> T fromJson(final String json, Class<T> valueType) 
    		throws JsonParseException, JsonMappingException, IOException {
    	return mapper.readValue(json, valueType);
    }
    
    public static <T> T fromJson(final String json, JavaType valueType) 
    		throws JsonParseException, JsonMappingException, IOException {
    	return mapper.readValue(json, valueType);
    }
    
    public static <T> T fromJson(final String json, TypeReference<T> valueTypeRef) 
    		throws JsonParseException, JsonMappingException, IOException {
    	return mapper.readValue(json, valueTypeRef);
    }
    
    public static Map<String, Object> fromJson(final String json) 
    		throws JsonParseException, JsonMappingException, IOException {
    	return fromJson(json, hashMapTypeReference);
    }
}
