package org.grants.loaders.arc_loader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.isbar_software.researchdata.MetadataAPI;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.index.RestIndex;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;
import org.neo4j.rest.graphdb.util.Config;

import au.com.bytecode.opencsv.CSVReader;

public class Loader {

	private static final String COMPLETED_GRANTS_CSV_PATH = "arc/completed_projects.csv";
	private static final String COMPLETED_ROLES_CSV_PATH = "arc/completed_fellowships.csv";
	private static final String NEW_GRANTS_CSV_PATH = "arc/new_projects.csv";
	private static final String NEW_ROLES_CSV_PATH = "arc/new_fellowships.csv";

	//	private static final int MAX_REQUEST_PER_TRANSACTION = 1000;

	
	private static final String LABEL_ARC_GRANT = "ARC_Grant";
	private static final String LABEL_ARC_RESEARCHER = "ARC_Researcher";
	private static final String LABEL_IDENTIFIER = "Identifier";
	private static final String LABEL_INSTITUTION = "Institution";
	
	//private static final String RELATIONSHIP_IDENTIFIES = "IDENTIFIES";
	
	private static final String FIELD_IDENTIFIER = "identifier";
	private static final String FIELD_TYPE = "type";

	private static final String FIELD_NAME = "name";
	private static final String FIELD_STATE = "state";
	
	private static final String FIELD_ARC_PROJECT_ID = "arc_grant_id";
	private static final String FIELD_ARC_SCHEME = "arc_scheme";
	private static final String FIELD_APPLICATION_YEAR = "application_year";
	private static final String FIELD_SCIENTIFIC_TITLE = "scientific_title";
	private static final String FIELD_START_YEAR = "start_year";
	private static final String FIELD_TOTAL_BUDGET = "total_budget";
	private static final String FIELD_RESEARCH_AREA = "research_area";
	private static final String FIELD_RESEARCH_AREA_CODE = "research_area_code";
	private static final String FIELD_DESCRIPTION = "description";
	
	private static final String FIELD_FULL_NAME = "full_name";
	private static final String FIELD_ARC_PERSONAL_ID = "arc_personal_id";
	
	private static final String[] TITLES = { "Mr ", "Ms ", "Dr ", "A/Prof ", "Adj/Prof ", "Asst Prof ", "Prof Dr ", "Prof " }; 
	
    private static enum RelTypes implements RelationshipType
    {
        IDENTIFIES, ADMIN_INSTITUTE, INVESTIGATOR
    }

    private static enum Labels implements Label {
    	Identifier, Institution, ARC_Grant, ARC_Researcher
    };
    
    public boolean LoadCsv(RestAPI graphDb,final String csv) {
    	// Obtain an index on Grant
		RestIndex<Node> indexGrant = graphDb.index().forNodes(LABEL_ARC_GRANT);
		
		// Obtain an index on Researcher
		RestIndex<Node> indexGrantee = graphDb.index().forNodes(LABEL_ARC_RESEARCHER);

		// Obtain an index on Identifier
		RestIndex<Node> indexIdentifier = graphDb.index().forNodes(LABEL_IDENTIFIER);

		// Obtain an index on Institution
		RestIndex<Node> indexInstitution = graphDb.index().forNodes(LABEL_INSTITUTION);
		
		// Obtain an index on Identifies relationship
	//	RelationshipIndex indexIdentified = graphDb.index().forRelationships(RELATIONSHIP_IDENTIFIES);

		// Imoprt Grant data
		System.out.println("Importing Grant data");
		long grantsCounter = 0;
	//	long transactionCount = 0;
		long beginTime = System.currentTimeMillis();
		
		Map<String, RestNode> mapIdentifiers = new HashMap<String, RestNode>();
		Map<String, RestNode> mapInstitution = new HashMap<String, RestNode>();
		Map<String, RestNode> mapGrants = new HashMap<String, RestNode>();
		Map<String, RestNode> mapReserachers = new HashMap<String, RestNode>();
		
	//	
		MetadataAPI metadata = new MetadataAPI();
		
		// process grats data file
		CSVReader reader;
		try 
		{
			reader = new CSVReader(new FileReader(csv));
			String[] grant;
			boolean header = false;
			while ((grant = reader.readNext()) != null) 
			{
				if (!header)
				{
					header = true;
					continue;
				}
				if (grant.length != 29)
					continue;
				
				String projectId = grant[0];
				System.out.println("Project id: " + projectId);			
				
				if (!mapGrants.containsKey(projectId)) {					
					String adminInstitute = grant[4];
				//	System.out.println("Administration institute: " + adminInstitute);
					
					RestNode nodeInstitution = mapInstitution.get(adminInstitute);
					if (null == nodeInstitution) {					
				//		System.out.println("Query administration institute NLA");
					
						metadata.query = "class:(party) AND display_title:(\"" + adminInstitute +  "\") AND identifier_type:\"AU-ANL:PEAU\"";
						metadata.fields = "identifier_value,display_title";
						
						Set<String> nlas = metadata.QueryField("identifier_value", "display_title", adminInstitute);
						List<RestNode> listIdentifiers = null;
						
						// let's check what we have identifier ready.
						if (nlas != null) {	
						//	System.out.println("We have found " + nlas.size() + " NLA");	

							for (String nla : nlas) {
							//	System.out.println("NLA: " + nla);	
								RestNode nodeIndex = mapIdentifiers.get(nla);
								if (null == nodeIndex) {
									Map<String, Object> map = new HashMap<String, Object>();
									map.put(FIELD_IDENTIFIER, nla);
									map.put(FIELD_TYPE, "NLA");
									
									nodeIndex = graphDb.getOrCreateNode(
											indexIdentifier, FIELD_IDENTIFIER, nla, map);
									if (!nodeIndex.hasLabel(Labels.Identifier))
										nodeIndex.addLabel(Labels.Identifier); 
									mapIdentifiers.put(nla, nodeIndex);	
								}
								
								if (null == listIdentifiers)
									listIdentifiers = new ArrayList<RestNode>();
								
								listIdentifiers.add(nodeIndex);
							} 
							
							// if we have find existing nodes, lets find what they points into same
							// institution node.
							if (null != listIdentifiers) {
							
								for (RestNode idx : listIdentifiers) {
									Iterable<Relationship> rels = idx.getRelationships(RelTypes.IDENTIFIES);							
									for (Relationship rel : rels) {
										RestNode n = (RestNode) rel.getEndNode();
										if (null != n) {
											if (null == nodeInstitution)
												nodeInstitution = n;
											else if (nodeInstitution.getId() != n.getId()) {
												reader.close();
												throw new Exception("Error, the relationship of index points on different node!");
											}
										}
									}
								}
							} 						
						}

						// if we didn't find any node, we will need to create it
					
						if (null == nodeInstitution) {
					//		System.out.println("Creating new administration institute");
						
							Map<String, Object> map = new HashMap<String, Object>();
							map.put(FIELD_NAME, adminInstitute);
							map.put(FIELD_STATE, grant[5]);
							//map.put(FIELD_TYPE, grant[8]);
							
							nodeInstitution = graphDb.getOrCreateNode(
									indexInstitution, FIELD_NAME, adminInstitute, map);
							if (!nodeInstitution.hasLabel(Labels.Institution))
								nodeInstitution.addLabel(Labels.Institution); 
							mapInstitution.put(adminInstitute, nodeInstitution);	
						}
						
						if (null != listIdentifiers) 
							for (RestNode idx : listIdentifiers) 
								if (!idx.hasRelationship(RelTypes.IDENTIFIES))
									idx.createRelationshipTo(nodeInstitution, RelTypes.IDENTIFIES);
					}
					
					Map<String, Object> map = new HashMap<String, Object>();
					map.put(FIELD_ARC_PROJECT_ID, projectId);
					map.put(FIELD_ARC_SCHEME, grant[1]);
					int applicationYear = 0;
					
					try {
						applicationYear = Integer.parseInt(grant[2]);
					} catch(Exception e) {}
					
					if (applicationYear != 0)
						map.put(FIELD_APPLICATION_YEAR, applicationYear);
					
					int commitmentYear = 0;
					try {
						commitmentYear = Integer.parseInt(grant[3]);
					} catch(Exception e) {}
					
					if (commitmentYear != 0)
						map.put(FIELD_START_YEAR, commitmentYear);
					
					map.put(FIELD_SCIENTIFIC_TITLE, grant[7]);
					map.put(FIELD_DESCRIPTION, grant[8]);
					map.put(FIELD_RESEARCH_AREA_CODE, grant[9]);
					map.put(FIELD_RESEARCH_AREA, grant[10]);
					map.put(FIELD_TOTAL_BUDGET, grant[11]);
				
					RestNode nodeGrant = graphDb.getOrCreateNode(indexGrant, FIELD_ARC_PROJECT_ID, projectId, map);
					if (!nodeGrant.hasLabel(Labels.ARC_Grant))
						nodeGrant.addLabel(Labels.ARC_Grant); 									
					if (!nodeGrant.hasRelationship(RelTypes.ADMIN_INSTITUTE))
						nodeGrant.createRelationshipTo(nodeInstitution, RelTypes.ADMIN_INSTITUTE);
										
					mapGrants.put(projectId, nodeGrant);	

					
					String investigator = grant[6];
					if (!investigator.contains("n.a.")) {
						
						List<String> investigators = null;
						if (investigator.contains(";"))
							investigators = Arrays.asList(investigator.split("-"));
						else
						{
							investigators = new ArrayList<String>();
							
							int tagSize = 0;
							while (null != investigator && !investigator.isEmpty()) {
								int pos = -1;
								int size = 0;
								for (String title : TITLES) {
									int pos1 = investigator.indexOf(title, tagSize);
									if (pos1 != -1 && (pos == -1 || pos > pos1)) {
										pos = pos1;
										size = title.length();
									}
								}
								if (pos != -1) {
									// we have find a tag
									tagSize = size;
									
									if (pos > 0) {									
										String inv = investigator.substring(0, pos).trim();
										investigator = investigator.substring(pos);
										if (!inv.isEmpty()) {
										//	System.out.println(inv);
											investigators.add(inv);
										}
									}
								} else {
									// we didn't find any 
									
									investigator = investigator.trim();
									if (!investigator.isEmpty()) {
								//		System.out.println(investigator);
										investigators.add(investigator);
									}
									investigator = null;
								}
							}
						}
						
						if (null != investigators && !investigators.isEmpty()) {
							for (String grantee : investigators) {
								String personalId = projectId + ":" + grantee;
								RestNode nodeGrantee = mapReserachers.get(personalId);
								if (null == nodeGrantee)
								{	
									
									map = new HashMap<String, Object>();
									map.put(FIELD_ARC_PERSONAL_ID, personalId);
									map.put(FIELD_FULL_NAME, grantee);
									
									nodeGrantee = graphDb.getOrCreateNode(indexGrantee, FIELD_ARC_PERSONAL_ID, personalId, map);
									if (!nodeGrantee.hasLabel(Labels.ARC_Researcher))
										nodeGrantee.addLabel(Labels.ARC_Researcher);
									if (!nodeGrantee.hasRelationship(RelTypes.INVESTIGATOR))
										nodeGrantee.createRelationshipTo(nodeGrant, RelTypes.INVESTIGATOR);
									mapReserachers.put(grantee, nodeGrantee);	
								}								
							}
						}
					}
				}
				else
					System.out.println("The Grants map already contains the key: " + projectId);
				
				/*
				
				
					
				// create grant node
				
				*/
				++grantsCounter;
			
			}
			
			reader.close();			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			
			return false;
		} catch (IOException e) {
			e.printStackTrace();
	
			return false;
		} catch (Exception e) {
			e.printStackTrace();
			
			return false;
		}
		
		long endTime = System.currentTimeMillis();
		
		System.out.println(String.format("Done. Imporded %d grants over %d ms. Average %f ms per grant", 
				grantsCounter, endTime - beginTime, (float)(endTime - beginTime) / (float)grantsCounter));

		
		return true;
    }
    
	public void Load(final String serverRoot)
	{
		System.setProperty(Config.CONFIG_STREAM, "true");
	
		// connect to graph database
		RestAPI graphDb = new RestAPIFacade(serverRoot);  
		
		//GraphDatabaseService graphDb = new RestGraphDatabase(serverRoot);  
			// create a query engine
		RestCypherQueryEngine engine=new RestCypherQueryEngine(graphDb);  
		
		// make sure we have an index on Grant:grant_id
		// RestAPI does not supported indexes created by schema, so we will use Cypher for that
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_ARC_GRANT + ") ASSERT n." + FIELD_ARC_PROJECT_ID + " IS UNIQUE", Collections.<String, Object> emptyMap());
		
		// make sure we have an index on Grantee:grant_id
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_ARC_RESEARCHER + ") ASSERT n."+ FIELD_ARC_PERSONAL_ID + " IS UNIQUE", Collections.<String, Object> emptyMap());

		// make sure we have an index on idx:Idet
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_IDENTIFIER + ") ASSERT n." + FIELD_IDENTIFIER + " IS UNIQUE", Collections.<String, Object> emptyMap());

		// make sure we have an index on idx:Identifier:identifier
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_INSTITUTION + ") ASSERT n." + FIELD_NAME + " IS UNIQUE", Collections.<String, Object> emptyMap());
				
	/*	if (!LoadCsv(graphDb, COMPLETED_GRANTS_CSV_PATH))
			return;*/
		
		if (!LoadCsv(graphDb, NEW_GRANTS_CSV_PATH))
			return;
	}
	
}
