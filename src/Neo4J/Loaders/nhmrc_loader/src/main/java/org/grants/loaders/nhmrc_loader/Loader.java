package org.grants.loaders.nhmrc_loader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Transaction;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;
import org.neo4j.rest.graphdb.util.QueryResult;

import au.com.bytecode.opencsv.CSVReader;

public class Loader {
	
		
	// CSV files are permanent, no problem with defining it
	private static final String GRANTS_CSV_PATH = "nhmrc/2014/grants-data.csv";
	private static final String ROLES_CSV_PATH = "nhmrc/2014/ci-roles.csv";
	private static final int MAX_REQUEST_PER_TRANSACTION = 1000;
	
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

	public void Load(final String serverRoot)
	{
		// connect to graph database
		RestAPI graphDb = new RestAPIFacade(serverRoot);  
		//GraphDatabaseService graphDb = new RestGraphDatabase(serverRoot);  
		
		// create a query engine
		RestCypherQueryEngine engine=new RestCypherQueryEngine(graphDb);  
		
		// make sure we have an index on Grant:grant_id
		engine.query("CREATE INDEX ON :Grant(grant_id)", Collections.EMPTY_MAP);
		
		// make sure we have an index on Grantee:grant_id
		engine.query("CREATE INDEX ON :Grantee(grant_id)", Collections.EMPTY_MAP);
		
						
		// Imoprt Grant data
		System.out.println("Importing Grant data");
		long grantsCounter = 0;
	//	long transactionCount = 0;
		long beginTime = System.currentTimeMillis();
		
		// process grats data file
		CSVReader reader;
		try 
		{
			//Transaction tx = graphDb.beginTx(); 
			
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
				
				/*
				if (transactionCount > MAX_REQUEST_PER_TRANSACTION)
				{
					tx.success();  
					tx.finish(); 
					tx = graphDb.beginTx(); 
				}*/
				
				Map<String, Object> map = new HashMap<String, Object>();
				map.put(FIELD_GRANT_ID, Integer.parseInt(grant[0]));
				map.put(FIELD_APPLICATION_YEAR, Integer.parseInt(grant[1]));
				map.put(FIELD_SUB_TYPE, grant[2]);
				map.put(FIELD_HIGHER_GRANT_TYPE, grant[3]);
				map.put(FIELD_ADMIN_INSTITUTION, grant[6]);
				map.put(FIELD_ADMIN_INSTITUTION_STATE, grant[7]);
				map.put(FIELD_ADMIN_INSTITUTION_TYPE, grant[8]);
				map.put(FIELD_SCIENTIFIC_TITLE, grant[9]);
				map.put(FIELD_SIMPLIFIED_TITLE, grant[10]);
				map.put(FIELD_CIA_NAME, grant[11]);
				map.put(FIELD_START_YEAR, Integer.parseInt(grant[12]));
				map.put(FIELD_END_YEAR, Integer.parseInt(grant[13]));
				map.put(FIELD_TOTAL_BUDGET, grant[41]);
				map.put(FIELD_RESEARCH_AREA, grant[42]);
				map.put(FIELD_FOR_CATEGORY, grant[43]);
				map.put(FIELD_OF_RESEARCH, grant[44]);
				
				List<String> keywords = null;
				for (int i = 45; i <= 49; ++i)
					if (grant[i].length() > 0)
					{
						if (null == keywords)
							keywords = new ArrayList<String>();
						keywords.add(grant[i]);
					}
					
				if (null != keywords)
					map.put(FIELD_KEYWORDS, keywords );
				
				keywords = null; 
				for (int i = 50; i <= 54; ++i)
					if (grant[i].length() > 0)
					{
						if (null == keywords)
							keywords = new ArrayList<String>();
						keywords.add(grant[i]);
					}
					
				if (null != keywords)
					map.put(FIELD_HEALTH_KEYWORDS, keywords );
				
		
				map.put(FIELD_MEDIA_SUMMARY, grant[54]);
				map.put(FIELD_SOURCE_SYSTEM, grant[55]);
				
				Map<String,Object> params=new HashMap<String, Object>();
				params.put("props", map);
					
				engine.query("CREATE (:Grant {props})", params);
				
				++grantsCounter;
			//	++transactionCount;
				
			/*	if (grantsCounter > 100)
					break;*/
			}
			
		/*	tx.success();  
			tx.finish(); */
	
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
	
		

		long granteesCounter = 0;
		beginTime = System.currentTimeMillis();
	
		try 
		{
			reader = new CSVReader(new FileReader(ROLES_CSV_PATH));
			String[] grantee;
			boolean header = false;
			while ((grantee = reader.readNext()) != null) 
			{
				if (!header)
				{
					header = true;
					continue;
				}
				if (grantee.length != 12)
					continue;
				
			/*	Map<String, Object> par = new HashMap<String, Object>();
				par.put(FIELD_GRANT_ID, Integer.parseInt(grantee[0]));*/
				
				Map<String, Object> map = new HashMap<String, Object>();
				map.put(FIELD_GRANT_ID, Integer.parseInt(grantee[0]));
				map.put(FIELD_ROLE, grantee[1]);
				map.put(FIELD_DW_INDIVIDUAL_ID, grantee[2]);
				map.put(FIELD_SOURCE_INDIVIDUAL_ID, grantee[3]);
				map.put(FIELD_TITLE, grantee[4]);
				map.put(FIELD_FIRST_NAME, grantee[5]);
				map.put(FIELD_MIDDLE_NAME, grantee[6]);
				map.put(FIELD_LAST_NAME, grantee[7]);
				map.put(FIELD_FULL_NAME, grantee[8]);
				map.put(FIELD_ROLE_START_DATE, grantee[9]);
				map.put(FIELD_ROLE_END_DATE, grantee[10]);
				map.put(FIELD_SOURCE_SYSTEM, grantee[11]);
				
				Map<String,Object> params=new HashMap<String, Object>();
				//params.put("props", par);
				params.put("props", map);
					
				QueryResult<Map<String,Object>> result = engine.query("CREATE (n:Grantee {props} RETURN n)", params);		
				Iterator<Map<String, Object>> iterator=result.iterator();  
				if(iterator.hasNext()) {  
					Map<String,Object> row = iterator.next();  
					
//					out.print("Total nodes: " + row.get("total"));  
				}  
				++granteesCounter;
				
				/*if (granteesCounter > 100)
					break;*/
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
		
		endTime = System.currentTimeMillis();
		
		System.out.println(String.format("Done. Imporded %d grantees and create relationships over %d ms. Average %f ms per grantee", 
				granteesCounter, endTime - beginTime, (float)(endTime - beginTime) / (float)granteesCounter));

		/*
		beginTime = System.currentTimeMillis();
		
		engine.query("MATCH (grantee:Grantee), (grant:Grant) WHERE grantee.grant_id = grant.grant_id CREATE (grantee)-[r:ROLE]->(grant)", Collections.EMPTY_MAP);
			
		endTime = System.currentTimeMillis();
		
		System.out.println(String.format("Done. Imporded %d grantees and create relationships over %d ms. Average %f ms per grantee", 
				granteesCounter, endTime - beginTime, (float)(endTime - beginTime) / (float)granteesCounter));

	*/
	}
	
}
