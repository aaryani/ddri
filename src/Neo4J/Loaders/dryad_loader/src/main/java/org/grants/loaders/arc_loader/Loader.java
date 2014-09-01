package org.grants.loaders.arc_loader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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

	private static final String CSV_PATH = "dryad/dryad.csv";

	private static final String LABEL_GRANT = "Grant";
	private static final String LABEL_RESEARCHER = "Researcher";
	private static final String LABEL_DATA_SET = "Dataset";
	
	private static final String LABEL_DRYAD = "Dryad";
	private static final String LABEL_DRYAD_RESEARCHER = LABEL_DRYAD + "_" + LABEL_RESEARCHER;
	private static final String LABEL_DRYAD_DATA_SET = LABEL_DRYAD + "_" + LABEL_DATA_SET;
	
	private static final String LABEL_RDA = "RDA";
	private static final String LABEL_RDA_RESEARCHER = LABEL_RDA + "_" + LABEL_RESEARCHER;
	
	private static final String LABEL_PURL = "PURL";
	private static final String LABEL_PURL_GRANT = LABEL_PURL + "_" + LABEL_GRANT;
	
	private static final String LABEL_NLA = "NLA";
	private static final String LABEL_NLA_RESEARCHER = LABEL_NLA + "_" + LABEL_RESEARCHER;

	private static final String LABEL_HDL = "HDL";
	private static final String LABEL_HDL_RESEARCHER = LABEL_HDL + "_" + LABEL_RESEARCHER;
	
	private static final String FIELD_DOI = "doi"; // Dryad Object Id
	
	private static final String FIELD_RDA = "rda";
	
	private static final String FIELD_PURL = "purl";
	
	private static final String FIELD_NLA = "nla";
	
	private static final String FIELD_HDL = "hdl";
	
	private static final String FIELD_FIRST_NAME = "first_name";
	private static final String FIELD_FULL_NAME = "full_name";
	private static final String FIELD_LAST_NAME = "last_name";
	private static final String FIELD_UNI_URL = "university_url";
	private static final String FIELD_UNI_HAVE_GRANTS = "have_grants";
	private static final String FIELD_EMAIL = "email";
	private static final String FIELD_SOCIAL = "social";
		
    private static enum RelTypes implements RelationshipType
    {
        Investigator, KnownAs, RelatedTo
    }

    private static enum Labels implements Label {
    	Dryad, RDA, PURL, NLA, HDL, Researcher, Grant, Dataset
    };
    
    private Map<String, RestNode> mapDataSets = new HashMap<String, RestNode>();
    private Map<String, RestNode> mapResearchers = new HashMap<String, RestNode>();
    private Map<String, RestNode> mapPurlGrants = new HashMap<String, RestNode>();
        
	public void Load(final String serverRoot)
	{
		System.setProperty(Config.CONFIG_STREAM, "true");
	
		// connect to graph database
		RestAPI graphDb = new RestAPIFacade(serverRoot);  
		
		//GraphDatabaseService graphDb = new RestGraphDatabase(serverRoot);  
			// create a query engine
		RestCypherQueryEngine engine=new RestCypherQueryEngine(graphDb);  
		
		// make sure we have an unique index on Dryad_Researcher:full_name
		// RestAPI does not supported indexes created by schema, so we will use Cypher for that
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_DRYAD_RESEARCHER + ") ASSERT n." + FIELD_FULL_NAME + " IS UNIQUE", Collections.<String, Object> emptyMap());
		
		// make sure we have an unique index on RDA_Researcher:rda
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_RDA_RESEARCHER + ") ASSERT n." + FIELD_RDA + " IS UNIQUE", Collections.<String, Object> emptyMap());
		
		// make sure we have an unique index on NLA_Researcher:nla
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_NLA_RESEARCHER + ") ASSERT n." + FIELD_NLA + " IS UNIQUE", Collections.<String, Object> emptyMap());

		// make sure we have an unique index on HDL_Researcher:hdl
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_HDL_RESEARCHER + ") ASSERT n." + FIELD_HDL + " IS UNIQUE", Collections.<String, Object> emptyMap());

		// make sure we have an unique index on Dryad_DataSet:doi
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_DRYAD_DATA_SET + ") ASSERT n."+ FIELD_DOI + " IS UNIQUE", Collections.<String, Object> emptyMap());

		// make sure we have an index on PLUR_GRant:plur
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_PURL_GRANT + ") ASSERT n."+ FIELD_PURL + " IS UNIQUE", Collections.<String, Object> emptyMap());

		
		
		if (!LoadCsv(graphDb, CSV_PATH))
			return;
	}
    
    private boolean LoadCsv(RestAPI graphDb,final String csv) {
    	
    	// Obtain an index on Dryad Researcher
		RestIndex<Node> indexResearcher = graphDb.index().forNodes(LABEL_DRYAD_RESEARCHER);
		
		// Obtain an index on Rda Researcher
		RestIndex<Node> indexRdaResearcher = graphDb.index().forNodes(LABEL_RDA_RESEARCHER);

		// Obtain an index on Nla Researcher
		RestIndex<Node> indexNlaResearcher = graphDb.index().forNodes(LABEL_NLA_RESEARCHER);
		
		// Obtain an index on Hdl Researcher
		RestIndex<Node> indexHdlResearcher = graphDb.index().forNodes(LABEL_HDL_RESEARCHER);
		
		// Obtain an index on Dryad DataSet
		RestIndex<Node> indexDryadDataSet = graphDb.index().forNodes(LABEL_DRYAD_DATA_SET);

		// Obtain an index on PLUR Grant
		RestIndex<Node> indexPurlGrant = graphDb.index().forNodes(LABEL_PURL_GRANT);

		
		// Imoprt Grant data
		System.out.println("Importing Dryard data");
		long recordsCounter = 0;
		long beginTime = System.currentTimeMillis();
		
		// process grats data file
		CSVReader reader;
		try 
		{
			reader = new CSVReader(new FileReader(csv));
			String[] record;
			boolean header = false;
			while ((record = reader.readNext()) != null) 
			{
				if (!header)
				{
					header = true;
					continue;
				}
				if (record.length != 20)
					continue;
				
				// decode data

				String doi = record[0];
				
				String firstName = record[1];
				String lastName = record[2];
				String fullName = record[3];
				
				String rda1 = record[6];
				String rda2 = record[7];
				
				String grant1 = record[8];
				String grant2 = record[9];
				String grant3 = record[10];
				String grant4 = record[11];
				String grant5 = record[12];
				
				String uni = record[13];
				String uniHaveGrants = record[14];
				
				String email = record[16];
				String nla = record[17];
				String hdl = record[18];
				String social = record[19];
				
				System.out.println("Researcher: " + fullName);	
				System.out.println("DOI: " + doi);	
				
				// Get or create researcher by his name				
				RestNode nodeResearcher = mapResearchers.get(fullName);
				if (null == nodeResearcher) {
					Map<String, Object> map = new HashMap<String, Object>();
					map.put(FIELD_FIRST_NAME, firstName);
					map.put(FIELD_LAST_NAME, lastName);
					map.put(FIELD_FULL_NAME, fullName);
					map.put(FIELD_EMAIL, email);
					map.put(FIELD_SOCIAL, social);
					
					if (null != uni && !uni.isEmpty()) {
						map.put(FIELD_UNI_URL, uni);
						if (null != uniHaveGrants && !uniHaveGrants.isEmpty()) {
							map.put(FIELD_UNI_HAVE_GRANTS, uniHaveGrants.equals("Y"));
						}
					}
					
					nodeResearcher = graphDb.getOrCreateNode(
							indexResearcher, FIELD_FULL_NAME, fullName, map);
					if (!nodeResearcher.hasLabel(Labels.Researcher))
						nodeResearcher.addLabel(Labels.Researcher); 
					if (!nodeResearcher.hasLabel(Labels.Dryad))
						nodeResearcher.addLabel(Labels.Dryad); 
					mapResearchers.put(fullName, nodeResearcher);	
				}
				
				ProcessRda(graphDb, indexRdaResearcher, nodeResearcher, fullName, rda1); 
				ProcessRda(graphDb, indexRdaResearcher, nodeResearcher, fullName, rda2); 
				
				ProcessNla(graphDb, indexNlaResearcher, nodeResearcher, fullName, nla); 
				
				ProcessHdl(graphDb, indexHdlResearcher, nodeResearcher, fullName, hdl);
				
				ProcessGrant(graphDb, indexPurlGrant, nodeResearcher, grant1); 
				ProcessGrant(graphDb, indexPurlGrant, nodeResearcher, grant2);
				ProcessGrant(graphDb, indexPurlGrant, nodeResearcher, grant3);
				ProcessGrant(graphDb, indexPurlGrant, nodeResearcher, grant4);
				ProcessGrant(graphDb, indexPurlGrant, nodeResearcher, grant5);
				
				ProcessDataSet(graphDb, indexDryadDataSet, nodeResearcher, doi);

				++recordsCounter;			
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
		
		System.out.println(String.format("Done. Imporded %d records over %d ms. Average %f ms per recortd", 
				recordsCounter, endTime - beginTime, (float)(endTime - beginTime) / (float)recordsCounter));
		
		return true;
    }

	private RestNode GetOrCreateRDAResearcher(RestAPI graphDb, RestIndex<Node> indexRdaResearcher, 
			String fullName, String rda) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(FIELD_RDA, rda);
		map.put(FIELD_FULL_NAME, fullName);
		
		RestNode nodeRda = graphDb.getOrCreateNode(
				indexRdaResearcher, FIELD_RDA, rda, map);
		if (!nodeRda.hasLabel(Labels.Researcher))
			nodeRda.addLabel(Labels.Researcher); 
		if (!nodeRda.hasLabel(Labels.RDA))
			nodeRda.addLabel(Labels.RDA); 
		
		return nodeRda;
	}
	
	private RestNode GetOrCreateNLAResearcher(RestAPI graphDb, RestIndex<Node> indexNlaResearcher, 
			String fullName, String nla) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(FIELD_NLA, nla);
		map.put(FIELD_FULL_NAME, fullName);
		
		RestNode nodeNla = graphDb.getOrCreateNode(
				indexNlaResearcher, FIELD_NLA, nla, map);
		if (!nodeNla.hasLabel(Labels.Researcher))
			nodeNla.addLabel(Labels.Researcher); 
		if (!nodeNla.hasLabel(Labels.NLA))
			nodeNla.addLabel(Labels.NLA); 
		
		return nodeNla;
	}
	
	private RestNode GetOrCreateHDLResearcher(RestAPI graphDb, RestIndex<Node> indexHdlResearcher, 
			String fullName, String hdl) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(FIELD_HDL, hdl);
		map.put(FIELD_FULL_NAME, fullName);
		
		RestNode nodeHdl = graphDb.getOrCreateNode(
				indexHdlResearcher, FIELD_NLA, hdl, map);
		if (!nodeHdl.hasLabel(Labels.Researcher))
			nodeHdl.addLabel(Labels.Researcher); 
		if (!nodeHdl.hasLabel(Labels.HDL))
			nodeHdl.addLabel(Labels.HDL); 
		
		return nodeHdl;
	}
	
	private RestNode GetOrCreatePLURGrant(RestAPI graphDb, RestIndex<Node> indexPurlGrant, String purl) {
		RestNode nodePurl = mapPurlGrants.get(purl);
		if (null == nodePurl) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put(FIELD_PURL, purl);
			
			nodePurl = graphDb.getOrCreateNode(indexPurlGrant, FIELD_PURL, purl, map);
			if (!nodePurl.hasLabel(Labels.Grant))
				nodePurl.addLabel(Labels.Grant); 
			if (!nodePurl.hasLabel(Labels.PURL))
				nodePurl.addLabel(Labels.PURL); 
			
			mapPurlGrants.put(purl, nodePurl);
		}
		
		return nodePurl;
	} 
	
	private RestNode GetOrCreateDryadDataSet(RestAPI graphDb, RestIndex<Node> indexDryadDataSet, String doi) {
		RestNode nodeDataSet = mapDataSets.get(doi);
		if (null == nodeDataSet) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put(FIELD_DOI, doi);
			
			nodeDataSet = graphDb.getOrCreateNode(indexDryadDataSet, FIELD_DOI, doi, map);
			if (!nodeDataSet.hasLabel(Labels.Dataset))
				nodeDataSet.addLabel(Labels.Dataset); 
			if (!nodeDataSet.hasLabel(Labels.Dryad))
				nodeDataSet.addLabel(Labels.Dryad); 
			
			mapDataSets.put(doi, nodeDataSet);
		}
		
		return nodeDataSet;
	} 
	
	private void CreateUniqueRelationship(RestNode nodeStart, RestNode nodeEnd, 
			RelTypes type, boolean checkOpposite) {
		// get all node relationships. They should be empty for a new node
		Iterable<Relationship> rels = nodeStart.getRelationships(type);
		
		for (Relationship rel : rels) {
			long startId = rel.getStartNode().getId();
			long endId = rel.getEndNode().getId();
			
			// check that relationship exists
			if (startId == nodeStart.getId() && endId == nodeEnd.getId() || 
			    checkOpposite && startId == nodeEnd.getId() && endId == nodeStart.getId())
				return;
		} 
		
		nodeStart.createRelationshipTo(nodeEnd, type);
	}	
	
	private void ProcessRda(RestAPI graphDb, RestIndex<Node> indexRdaResearcher, RestNode nodeResearcher, 
			String fullName, String rda) {
		if (null != rda && !rda.isEmpty()) {
			RestNode nodeRda = GetOrCreateRDAResearcher(graphDb, indexRdaResearcher, fullName, rda);
			CreateUniqueRelationship(nodeResearcher, nodeRda, RelTypes.KnownAs, true);
		}
	}
	
	private void ProcessNla(RestAPI graphDb, RestIndex<Node> indexNlaResearcher, RestNode nodeResearcher, 
			String fullName, String nla) {
		if (null != nla && !nla.isEmpty()) {
			RestNode nodeNla = GetOrCreateNLAResearcher(graphDb, indexNlaResearcher, fullName, nla);
			CreateUniqueRelationship(nodeResearcher, nodeNla, RelTypes.KnownAs, true);
		}
	}
	
	private void ProcessHdl(RestAPI graphDb, RestIndex<Node> indexHdlResearcher, RestNode nodeResearcher, 
			String fullName, String hdl) {
		if (null != hdl && !hdl.isEmpty()) {
			RestNode nodeHdl = GetOrCreateHDLResearcher(graphDb, indexHdlResearcher, fullName, hdl);
			CreateUniqueRelationship(nodeResearcher, nodeHdl, RelTypes.KnownAs, true);
		}
	}
	
	private void ProcessGrant(RestAPI graphDb, RestIndex<Node> indexPurlGrant, RestNode nodeResearcher, 
			String grant) {
		if (null != grant && !grant.isEmpty()) {
			RestNode nodeGrant = GetOrCreatePLURGrant(graphDb, indexPurlGrant, grant);
			CreateUniqueRelationship(nodeResearcher, nodeGrant, RelTypes.Investigator, false);			
		}
	}	
	
	private void ProcessDataSet(RestAPI graphDb, RestIndex<Node> indexDryadDataSet, RestNode nodeResearcher, 
			String doi) {
		if (null != doi && !doi.isEmpty()) {
			RestNode nodeDataSet = GetOrCreateDryadDataSet(graphDb, indexDryadDataSet, doi);
			CreateUniqueRelationship(nodeResearcher, nodeDataSet, RelTypes.RelatedTo, false);			
		}
	}	
	
}
