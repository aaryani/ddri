package org.grants.loaders.nhmrc_loader;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.isbar_software.neo4j.Database;
import org.isbar_software.neo4j.node.Node;
import org.isbar_software.neo4j.schema.index.Index;
import org.isbar_software.neo4j.utils.Neo4JException;

import au.com.bytecode.opencsv.CSVReader;

public class Loader {
	  // CSV files are permanent, no problem with defining it
    private static final String GRANTS_CSV_PATH = "nhmrc/2014/grants-data.csv";
    private static final String ROLES_CSV_PATH = "nhmrc/2014/ci-roles.csv";

    private static final String LABEL_GRANT = "Grant";
    private static final String LABEL_RESEARCHER = "Researcher";

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
        
    public void Load(final String serverUri) 
    		throws JsonParseException, JsonMappingException, IOException, Neo4JException
    {
		Database db = new Database(serverUri);
		if (!db.checkServerStatus())
		{
			System.out.println("Unable to connect to the server: " + db.getServerUri());
			return;
		}
		
		Index grantIndex = db.indexes().createIndex(LABEL_GRANT, FIELD_GRANT_ID);
		Index researcherIndex = db.indexes().createIndex(LABEL_RESEARCHER, FIELD_DW_INDIVIDUAL_ID);
		
		System.out.println("Importing Grant data");
		long grantsCounter = 0;
		long beginTime = System.currentTimeMillis();
		
		Map<Integer, Node> mapGrants = new HashMap<Integer, Node>();
		
		// process grats data file
		CSVReader reader = new CSVReader(new FileReader(GRANTS_CSV_PATH));
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
			if (!mapGrants.containsKey(grantId))
			{					
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("labels", "Grant");
				map.put(FIELD_GRANT_ID, grantId);
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
				
				Node nodeGrant = db.nodes().createNodeWithLabelAndParameters(LABEL_GRANT, map);
				mapGrants.put(grantId, nodeGrant);
				
				
				/*
				
				Map<String, Object> prop = new HashMap<String, Object>();
				prop.put("prop", map);
				
				Map<String, Object> response = db.query("CREATE (n:Grant { prop }) RETURN n", prop);
				Iterator it = response.entrySet().iterator();
				 while (it.hasNext()) {
				        Map.Entry pairs = (Map.Entry)it.next();
				        System.out.println(pairs.getKey() + " = [" +  pairs.getValue().getClass() + "] " + pairs.getValue());
				        
				        List<Object> data =  (List<Object>) pairs.getValue();
				        for (int i = 0; i < data.size(); ++i)
				        {
				        	System.out.println(i + ":[" +  data.get(i).getClass() + "] " + data.get(i));
				        	
				        	List<Object> data1 =  (List<Object>) data.get(i);
				        	for (int j = 0; j < data1.size(); ++j)
					        {
				        		System.out.println(i + ":" + j + ":[" +  data1.get(i).getClass() + "] " + data1.get(i));
					        }
				        }
				        it.remove(); // avoids a ConcurrentModificationException
				    }
				 
				 int gotcha = 1;
				
			/*	Node nodeGrant = db.nodes().createNodeWithParameters(map);
				nodeGrant.setLabels(LABEL_GRANT);
				mapGrants.put(grantId, nodeGrant);*/
			}
			else
				System.out.println("The Grants map already contains the key: " + grantId);
				
			++grantsCounter;
			
			if (grantsCounter >= 1000)
				break;
		}
	
		reader.close();
		
		long endTime = System.currentTimeMillis();

		System.out.println(String.format("Done. Imporded %d grants over %d ms. Average %f ms per grant", 
                         grantsCounter, endTime - beginTime, (float)(endTime - beginTime) / (float)grantsCounter));



		
    }
}
