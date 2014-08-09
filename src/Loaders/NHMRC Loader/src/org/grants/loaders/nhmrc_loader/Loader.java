package org.grants.loaders.nhmrc_loader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.core.MediaType;

import au.com.bytecode.opencsv.CSVReader;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

public class Loader {
	// remove it for production version
	//private static final String SERVER_ROOT_URI = "http://54.83.73.225:7474/db/data/";
	private static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";
		
	// CSV files are permanent, no problem with defining it
	private static final String GRANTS_CSV_PATH = "nhmrc/2014/grants-data.csv";
	private static final String ROLES_CSV_PATH = "nhmrc/2014/ci-roles.csv";
	
	private static final String LABEL_GRANT = "Grant";
	private static final String LABEL_GRANTEE = "Grantee";
	
	private static final String FIELD_GRANT_ID = "grant_id";
	private static final String FIELD_APPLICATION_YEAR = "application_year";
	private static final String FIELD_SUB_TYPE = "sub_type";
	private static final String FIELD_HIGHER_GRANT_TYPE = "higher_grant_type";
	private static final String FIELD_SCIENTIFIC_TITLE = "scientific_title";
	private static final String FIELD_SIMPLIFIED_TITLE = "simplified_title";
	private static final String FIELD_CIA_NAME = "cia_name";
	private static final String FIELD_START_YEAR = "start_year";
	private static final String FIELD_END_YEAR = "end_year";
	private static final String FIELD_TOTAL_BUDGET = "total_budget";
	private static final String FIELD_RESEARCH_AREA = "research_area";
	private static final String FIELD_FOR_CATEGORY = "for_category";
	private static final String FIELD_OF_RESEARCH = "field_of_research";
	private static final String FIELD_ADMIN_INSTITUTION = "admin_institution";
	private static final String FIELD_ADMIN_INSTITUTION_STATE = "admin_institution_state";
	private static final String FIELD_ADMIN_INSTITUTION_TYPE = "admin_institution_type";
	private static final String FIELD_KEYWORDS = "keywords";
	private static final String FIELD_HEALTH_KEYWORDS = "health_keywords";
	private static final String FIELD_MEDIA_SUMMARY = "media_summary";
	private static final String FIELD_SOURCE_SYSTEM = "source_system";
	
	private static final String FIELD_ROLE = "role";
	private static final String FIELD_DW_INDIVIDUAL_ID = "dw_individual_id";
	private static final String FIELD_SOURCE_INDIVIDUAL_ID = "source_individual_id";
	private static final String FIELD_TITLE = "title";
	private static final String FIELD_FIRST_NAME = "first_name";
	private static final String FIELD_MIDDLE_NAME = "middle_name";
	private static final String FIELD_LAST_NAME = "last_name";
	private static final String FIELD_FULL_NAME = "full_name";
	private static final String FIELD_ROLE_START_DATE = "role_start_date";
	private static final String FIELD_ROLE_END_DATE = "role_end_date";
	
	private static enum IndexType
	{
	    NOT_UNIQUE, GET_OR_CREATE, CREATE_OR_FAIL
	}

	private static String sendTransactionalCypherQuery(final String serverUri, final String query) 
	{
		final String uri = serverUri + "transaction/commit";
		WebResource resource = Client.create().resource( uri );

		String queryJson = "{\"statements\" : [ {\"statement\" : \"" +query + "\"} ]}";
		ClientResponse response = resource
		        .accept( MediaType.APPLICATION_JSON )
		        .type( MediaType.APPLICATION_JSON )
		        .entity( queryJson )
		        .post( ClientResponse.class );
		
		int status = response.getStatus();
		final String entitity = response.getEntity( String.class );
		
		System.out.println( String.format(
		        "POST [%s] to [%s], status code [%d], returned data: "
		        		+ System.getProperty( "line.separator" ) + "%s",
                queryJson, uri, status, entitity) );

		response.close();
		
		return entitity;
	}
	
	private static Boolean createIndex(final String serverUri, final IndexType indexType, final String label, final String ... keys )
	{
		String uri = serverUri + "/schema/index/" + label;
		switch (indexType)
		{
		case GET_OR_CREATE:
			uri += "?uniqueness=get_or_create";
			break;
				
		case CREATE_OR_FAIL:
			uri += "?uniqueness=create_or_fail";
			break;
		
		default:
			break;
		}
		
		final String json = generateJsonIndex(keys);
	
		WebResource resource = Client.create()
				.resource( uri );
	
		ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
				.type( MediaType.APPLICATION_JSON )
				.entity( json )
				.post( ClientResponse.class );
	
		int status = response.getStatus();
		String entitity = response.getEntity( String.class );
		
		/*System.out.println( String.format(
		        "POST [%s] to [%s], status code [%d], returned data: "
		                + System.getProperty( "line.separator" ) + "%s",
                json, uri, status, entitity) );*/

		response.close();
		
		return status == 200;
	}
	
	public static URI createNode(final String serverUri)
	{
		final String uri = serverUri + "node";
		// http://localhost:7474/db/data/node

		WebResource resource = Client.create()
		        .resource( uri );
		// POST {} to the node entry point URI
		ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
		        .type( MediaType.APPLICATION_JSON )
		        .entity( "{}" )
		        .post( ClientResponse.class );

		int status = response.getStatus();
		final URI location = response.getLocation();
		/*System.out.println( String.format(
		        "POST to [%s], status code [%d], location header [%s]",
		        uri, status, location.toString() ) );*/
		response.close();
		
		if (status != 201)
			return null;

		return location;
	}
	
	private static Boolean addNodeLabels(final URI nodeUri, final String ... labels) throws Exception
	{
		final String uri = nodeUri.toString() + "/labels";
		// http://localhost:7474/db/data/node/n/labels
		final String json = generateJsonStringArray(labels);
		
		WebResource resource = Client.create()
		        .resource( uri );
		// POST {} to the node entry point URI
		ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
		        .type( MediaType.APPLICATION_JSON )
		        .entity( json )
		        .post( ClientResponse.class );

		int status = response.getStatus();
		
		/*System.out.println( String.format(
		        "POST [%s] to [%s], status code [%d]",
		        json, uri, status ) );
		response.close();*/
		
		return status == 204;
	}
	
	private static Boolean addNodeProperty(final URI nodeUri, final String propertyName, final Object ... propertyValues) throws Exception
	{
		final String uri = nodeUri.toString() + "/properties/" + propertyName;
		// http://localhost:7474/db/data/node/[n]/properties/[propertyName]
		final String json = generateJsonArray(propertyValues);
		
		WebResource resource = Client.create()
		        .resource( uri );
		// POST {} to the node entry point URI
		ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
		        .type( MediaType.APPLICATION_JSON )
		        .entity( json )
		        .put( ClientResponse.class );

		int status = response.getStatus();
		
	/*	System.out.println( String.format(
		        "POST [%s] to [%s], status code [%d]",
		        json, uri, status ) );
		response.close();*/
		
		return status == 204;
	}
	
	private static Boolean addNodeProperties(final URI nodeUri, final Map<String, Object> properties) throws Exception
	{
		final String uri = nodeUri.toString() + "/properties";
		// http://localhost:7474/db/data/node/[n]/properties/[propertyName]
		final String json = generateJsonMap(properties);
		
		WebResource resource = Client.create()
		        .resource( uri );
		// POST {} to the node entry point URI
		ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
		        .type( MediaType.APPLICATION_JSON )
		        .entity( json )
		        .put( ClientResponse.class );

		int status = response.getStatus();
		
		/*System.out.println( String.format(
		        "POST [%s] to [%s], status code [%d]",
		        json, uri, status ) );*/
		response.close();
		
		return status == 204;
	}
	
	private static Boolean checkIfNodeExists(final String serverUri, final String nodeLabel, 
			final String key, final Object value) throws Exception
	{
		final String uri = serverUri + "index/node/" + nodeLabel + "/" + key + "/" + generateUrl(value);
		
		WebResource resource = Client.create()
		        .resource( uri );
		// POST {} to the node entry point URI
		ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
		        .type( MediaType.APPLICATION_JSON )
		        .get( ClientResponse.class );

		int status = response.getStatus();
		
		/*System.out.println( String.format(
		        "POST [%s] to [%s], status code [%d]",
		        json, uri, status ) );*/
		response.close();
		
		return status == 200;
	}
	
	private static String generateJsonIndex( String ... keys ) 
	{			
		StringBuilder sb = new StringBuilder();
		sb.append( "{ \"property_keys\" : [" );
		for ( int i = 0; i < keys.length; i++ )
		{
			if ( i != 0 )
				sb.append( ", " );
			sb.append( "\"" );
			sb.append( keys[i] );
			sb.append( "\"" );
		}
		sb.append( "] }" );
			
		return sb.toString();
	}
	
	private static String generateJson( Object o ) throws Exception
	{
		if (o.getClass().equals(Integer.class))
			return Integer.toString((int)o);
		if (o.getClass().equals(Float.class))
			return Float.toString((float)o);
		if (o.getClass().equals(String.class))
			return "\"" + (String)o + "\""; 
		if (o.getClass().equals(Object[].class))
			return generateJsonArray((Object[]) o);
		if (o.getClass().equals(String[].class))
			return generateJsonStringArray((String[]) o);
		throw new Exception("JSON Object type is not supported: " + o.getClass());
	}
	
	private static String generateJsonArray( Object[] o) throws Exception
	{
		if (o.length == 0)
			throw new Exception("The JSON array can not be empty!");
		
		if (o.length == 1)
			return generateJson(o[0]);
		
		StringBuilder sb = new StringBuilder();
		sb.append( "[" );
		for ( int i = 0; i < o.length; i++ )
		{
			if ( i != 0 )
				sb.append( ", " );
			sb.append( generateJson(o[i]) );
		}
		sb.append( "]" );
		
		return sb.toString();		
	}
	
	private static String generateJsonString( String s)
	{
		return "\"" + s + "\"";
	}
	
	private static String generateJsonStringArray( String[] s) throws Exception
	{
		if (s.length == 0)
			throw new Exception("The JSON array can not be empty!");
		
		if (s.length == 1)
			return generateJsonString(s[0]);
		
		StringBuilder sb = new StringBuilder();
		sb.append( "[" );
		for ( int i = 0; i < s.length; i++ )
		{
			if ( i != 0 )
				sb.append( ", " );
			sb.append( generateJsonString( s[i] ) );
			
		}
		sb.append( "]" );
		
		return sb.toString();		
	}
	
	private static String generateJsonMap(Map<String, Object> map) throws Exception
	{
		StringBuilder sb = new StringBuilder();
		sb.append( "{" );
		
		Boolean needComma = false;
		
		Iterator<Entry<String, Object>> it = map.entrySet().iterator();
		while (it.hasNext()) {
	        Map.Entry<String, Object> pairs = (Map.Entry<String, Object>)it.next();
	 
	        if (needComma)
	        	sb.append(", ");
	        else
	        	needComma = true;
	        
	        sb.append(generateJsonString(pairs.getKey()));
	        sb.append(" : ");
	        sb.append(generateJson(pairs.getValue()));
	        
	        it.remove(); // avoids a ConcurrentModificationException
		}
	
		sb.append( "}" );
		
		return sb.toString();		
	}
	
	private static String generateUrl( Object o ) throws Exception
	{
		if (o.getClass().equals(Integer.class))
			return Integer.toString((int)o);		
		throw new Exception("URL Object type is not supported: " + o.getClass());
	}
	
	private static boolean checkServerStatus(final String serverUri) {
		WebResource resource = Client.create()
		        .resource( serverUri );
		ClientResponse response = resource.get( ClientResponse.class );

		System.out.println( String.format( "GET on [%s], status code [%d]",
		        SERVER_ROOT_URI, response.getStatus() ) );	
		int status = response.getStatus();
		
		response.close();
		
		return status == 200;
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// init server uri
		String serverUri = SERVER_ROOT_URI;
		if (args.length > 0)
			serverUri = args[0];
		
		// check what server uri has been supplied
		if (serverUri.length() == 0) {
			System.out.print( "Error: No server address has been specyfied. Please provide server addres." );
			return;
		}
		
		// check what server is online
		if (!checkServerStatus(serverUri))
		{
			System.out.print( "Error: Unable to connect to neo4j server at address: " + serverUri  + "." );
			return;
		}
		
		// create index on Grants:grant_id
		createIndex(serverUri, IndexType.GET_OR_CREATE, "Grant", "grant_id");
		
		System.out.println("Importing Grant data");
		long grantsCounter = 0;
		long beginTime = System.currentTimeMillis();
		
		// process grats data file
		CSVReader reader;
		try {
			reader = new CSVReader(new FileReader(GRANTS_CSV_PATH));
			String[] grant;
			boolean header = false;
			while ((grant = reader.readNext()) != null) 
			{
				if (!header)
				{
					header = true;
					continue;
				}
				if (grant.length != 57)
					continue;
				
				int grantId = Integer.parseInt(grant[0]);
				int applicationYear = Integer.parseInt(grant[1]);
				String subType = grant[2];
				String higherGrantType = grant[3];
			//	String mainFundingGroup = grant[4];
		//		String didFunded = grant[5];
				String adminInstitution = grant[6];
				String adminInstitutionState = grant[7];
				String adminInstitutionType = grant[8];
				String scientificTitle = grant[9];
				String simplifiedTitle = grant[10];
				String ciaName = grant[11];
				int startYear = Integer.parseInt(grant[12]);
				int endYear = Integer.parseInt(grant[13]);
				String totalBudget = grant[41];
				String researchArea = grant[42];
				String forCategory = grant[43];
				String fieldOfResearch = grant[44];
				String keyword1 = grant[45];
				String keyword2 = grant[46];
				String keyword3 = grant[47];
				String keyword4 = grant[48];
				String keyword5 = grant[49];
				String healthKeyword1 = grant[50];
				String healthKeyword2 = grant[51];
				String healthKeyword3 = grant[52];
				String healthKeyword4 = grant[53];
				String healthKeyword5 = grant[54];
				String mediaSummary = grant[54];
				String sourceSystem = grant[56];
				
			/*	if (checkIfNodeExists(serverUri, LABEL_GRANT, FIELD_GRANT_ID, grantId))
					continue; // already existing*/
				
				URI grantNode = createNode(serverUri);
				addNodeLabels(grantNode, LABEL_GRANT);		
				
				/*
				addNodeProperties(grantNode, FIELD_GRANT_ID, grantId);
				addNodeProperties(grantNode, FIELD_APPLICATION_YEAR, applicationYear);
				addNodeProperties(grantNode, FIELD_SUB_TYPE, subType);
				addNodeProperties(grantNode, FIELD_HIGHER_GRANT_TYPE, higherGrantType);
				addNodeProperties(grantNode, FIELD_ADMIN_INSTITUTION, adminInstitution);
				addNodeProperties(grantNode, FIELD_ADMIN_INSTITUTION_STATE, adminInstitutionState);
				addNodeProperties(grantNode, FIELD_ADMIN_INSTITUTION_TYPE, adminInstitutionType);
				addNodeProperties(grantNode, FIELD_SCIENTIFIC_TITLE, scientificTitle);
				addNodeProperties(grantNode, FIELD_SIMPLIFIED_TITLE, simplifiedTitle);
				addNodeProperties(grantNode, FIELD_CIA_NAME, ciaName);
				addNodeProperties(grantNode, FIELD_START_YEAR, startYear);
				addNodeProperties(grantNode, FIELD_END_YEAR, endYear);
				addNodeProperties(grantNode, FIELD_TOTAL_BUDGET, totalBudget);
				addNodeProperties(grantNode, FIELD_RESEARCH_AREA, researchArea);
				addNodeProperties(grantNode, FIELD_FOR_CATEGORY, forCategory);
				addNodeProperties(grantNode, FIELD_OF_RESEARCH, fieldOfResearch);
				addNodeProperties(grantNode, FIELD_KEYWORDS, keyword1, keyword2, keyword3, keyword4, keyword5);
				addNodeProperties(grantNode, FIELD_HEALTH_KEYWORDS, healthKeyword1, healthKeyword2, healthKeyword3, healthKeyword4, healthKeyword5);
				addNodeProperties(grantNode, FIELD_MEDIA_SUMMARY, mediaSummary);
				addNodeProperties(grantNode, FIELD_SOURCE_SYSTEM, sourceSystem);
				*/
				
				Map<String, Object> map = new HashMap<String, Object>();
				map.put(FIELD_GRANT_ID, grantId);
				map.put(FIELD_APPLICATION_YEAR, applicationYear);
				map.put(FIELD_SUB_TYPE, subType);
				map.put(FIELD_HIGHER_GRANT_TYPE, higherGrantType);
				map.put(FIELD_ADMIN_INSTITUTION, adminInstitution);
				map.put(FIELD_ADMIN_INSTITUTION_STATE, adminInstitutionState);
				map.put(FIELD_ADMIN_INSTITUTION_TYPE, adminInstitutionType);
				map.put(FIELD_SCIENTIFIC_TITLE, scientificTitle);
				map.put(FIELD_SIMPLIFIED_TITLE, simplifiedTitle);
				map.put(FIELD_CIA_NAME, ciaName);
				map.put(FIELD_START_YEAR, startYear);
				map.put(FIELD_END_YEAR, endYear);
				map.put(FIELD_TOTAL_BUDGET, totalBudget);
				map.put(FIELD_RESEARCH_AREA, researchArea);
				map.put(FIELD_FOR_CATEGORY, forCategory);
				map.put(FIELD_OF_RESEARCH, fieldOfResearch);
				map.put(FIELD_KEYWORDS, new String[] { keyword1, keyword2, keyword3, keyword4, keyword5 } );
				map.put(FIELD_HEALTH_KEYWORDS, new String[] { healthKeyword1, healthKeyword2, healthKeyword3, healthKeyword4, healthKeyword5 } );
				map.put(FIELD_MEDIA_SUMMARY, mediaSummary);
				map.put(FIELD_SOURCE_SYSTEM, sourceSystem);
				
				addNodeProperties(grantNode, map);
				
				++grantsCounter;
				
				if (grantsCounter > 100)
					break;
			}
	
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			
			return;
		} catch (IOException e) {
			e.printStackTrace();

			return;
		} catch (Exception e) {
			e.printStackTrace();
			
			return;
		}
		
		long endTime = System.currentTimeMillis();
		
		System.out.println(String.format("Done. Imporded %d grants over %d ms. Average %f ms per grant", 
				grantsCounter, endTime - beginTime, (float)(endTime - beginTime) / (float)grantsCounter));
	}

}
