package org.grants.google.cse;

import java.io.IOException;

import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class Query {

	private String cseId;
	private String apiKey;
	
	private static final String CSE_API = "https://www.googleapis.com/customsearch/v1?key=%s&cx=%s&q=%s";
	private static final ObjectMapper mapper = new ObjectMapper();   
	
	public Query(final String cseId, final String apiKey) {
		this.cseId = cseId;
		this.apiKey = apiKey;
	}
	
	public QueryResponse query(final String queryString) {
		String url = String.format(CSE_API, apiKey, cseId, queryString);
		String json = get(url);
		
		System.out.println(json);
		
		try {
			return mapper.readValue(json, QueryResponse.class);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}
	
	private String get( final String url ) {
		System.out.println("Downloading: " + url);
				
		ClientResponse response = Client.create()
								  .resource( url )
								  .accept( MediaType.APPLICATION_JSON ) 
								  .get( ClientResponse.class );
		
		if (response.getStatus() == 200) 
			return response.getEntity( String.class );
		
		return null;
    } 
}
