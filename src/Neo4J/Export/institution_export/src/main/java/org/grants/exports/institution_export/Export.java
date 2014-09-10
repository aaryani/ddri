package org.grants.exports.institution_export;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.util.Config;

public class Export {

	/*private static final String LABEL_GRANT = "Grant";
	private static final String LABEL_RESEARCHER = "Researcher";
	private static final String LABEL_INSTITUTION = "Institution";
	private static final String LABEL_DATASET = "Dataset";
	
	/*
	private static final String LABEL_ARC = "ARC";
	private static final String LABEL_ARC_GRANT = LABEL_ARC + "_" + LABEL_GRANT;
	private static final String LABEL_ARC_RESEARCHER = LABEL_ARC + "_" + LABEL_RESEARCHER;
	private static final String LABEL_ARC_INSTITUTION = LABEL_ARC + "_" + LABEL_INSTITUTION;
	
	private static final String LABEL_NHMRC = "NHMRC";
	private static final String LABEL_NHMRC_GRANT = LABEL_NHMRC + "_" + LABEL_GRANT;
	private static final String LABEL_NHMRC_RESEARCHER = LABEL_NHMRC + "_" + LABEL_RESEARCHER;
	private static final String LABEL_NHMRC_INSTITUTION = LABEL_NHMRC + "_" + LABEL_INSTITUTION;
	
	private static final String LABEL_DRYAD = "Dryad";
	private static final String LABEL_DRYAD_RESEARCHER = LABEL_DRYAD + "_" + LABEL_RESEARCHER;
	private static final String LABEL_DRYAD_DATASET = LABEL_DRYAD + "_" + LABEL_DATASET;
	
	private static final String LABEL_WEB = "Web";
	private static final String LABEL_WEB_RESEARCHER = LABEL_WEB + "_" + LABEL_RESEARCHER;

	private static final String LABEL_RDA = "RDA";
	private static final String LABEL_RDA_INSTITUTION = LABEL_RDA + "_" + LABEL_INSTITUTION;*/
	
	private static final String FIELD_NAME = "name";
	private static final String FIELD_SCIENTIFIC_TITLE = "scientific_title";
	private static final String FIELD_FULL_NAME = "full_name";
	
	private static final String PROPERTY_SELF = "self"; 
	private static final String PROPERTY_NAME = "name";
	private static final String PROPERTY_CLASS = "class"; 
	private static final String PROPERTY_COLOR = "color"; 	
	private static final String PROPERTY_SIZE = "size"; 	
	private static final String PROPERTY_DATA = "data"; 
	private static final String PROPERTY_CHILDREN = "children"; 
	//private static final String PROPERTY_X = "x"; 
	//private static final String PROPERTY_Y = "y"; 
	
	private static final String COLOR_INSTITUTION = "#0066FF";
	private static final String COLOR_GRANT = "#FF4D4D";
	private static final String COLOR_RESEARCHER = "#336699";
	private static final String COLOR_DATASET = "#123456";
	
	private static final String CLASS_INSTITUTION = "Institution";
	private static final String CLASS_GRANT = "Grant";
	private static final String CLASS_RESEARCHER = "Researcher";
	private static final String CLASS_DATASET = "Dataset";
	
	private static final int MAX_GRANTS = 50; // 0 to query all grants
	//private static final int MAX_RESEARCHERS = 0; // 0 to query all grants
	private static final int MAX_NAME = 8;
	
	private static enum RelTypes implements RelationshipType
    {
    	AdminInstitute, Investigator, KnownAs, RelatedTo
    }
	
	private static final long INSTITUTION_IDS[] = { 0, 3, 5, 7, 10, 13, 16, 20, 22, 28 };
	
	// Qyery all instutuions ID:
	// MATCH (n:NHMRC:Institution) RETURN id(n) UNION MATCH (n:ARC:Institution) RETURN id(n) 
	
	public void ExportInstitutions(final String serverRoot)
	{
		new File("json").mkdirs();
		
		System.setProperty(Config.CONFIG_STREAM, "true");
		
		// connect to graph database
		RestAPI graphDb = new RestAPIFacade(serverRoot);  
		
		// create object mapper
		ObjectMapper mapper = new ObjectMapper();   
			
		// create map to store result;
		Map<String, String> mapNodes = new HashMap<String, String>();
		
		for (long nodeInstId : INSTITUTION_IDS) {
			// Get Institution node
			RestNode nodeInstitution = graphDb.getNodeById(nodeInstId);
			
			if (null == nodeInstitution) {
				System.out.println("Invalid node id: " + nodeInstId);
				continue;
			}

			System.out.println("Processing instutution: " + nodeInstitution.getProperty(FIELD_NAME) + " (" + nodeInstitution + ")");
			
			// Create Institution Map
			Map<String, Object> mapInstitution = CreateInstitutionMap(nodeInstitution);

			// Get all Grants
			Map<Long, RestNode> nodesGrants = QueryAllInstitutionGrants(nodeInstitution, null);
			
			// Query all KnwonAs Institutions
			Map<Long, RestNode> nodesInstitutions = QueryAllKnownAsNodes(nodeInstitution, null); 
			for (Map.Entry<Long, RestNode> entry : nodesInstitutions.entrySet()) {
				if (entry.getKey() != nodeInstitution.getId()) {
					// Get Known As Institution node
					RestNode node = entry.getValue();
			    
					// We will represent this institution as one node, so copy missing data to it
					CopyNodeData(node, mapInstitution);
					
					// Add all Grants to the node
					QueryAllInstitutionGrants(node, nodesGrants);
				}
			}
			
			int grantCounter = 0;
			
			// Add all grant nodes as a children
			for (RestNode nodeGrant : nodesGrants.values()) {
				// Create Grant Map. Grant does not have known as relationship, so this is it for now			
				Map<String, Object> mapGrant = CreateGrantMap(nodeGrant);

				// Query all grants researchers
				Map<Long, RestNode> nodesResearchers = QueryAllGrantResearchers(nodeGrant, null);
				
				// Create empty map for formatted researchers
				Set<Long> setResearchers = new HashSet<Long>();
				
				// Now enumerate researchers nodes
				for (Map.Entry<Long, RestNode> entry : nodesResearchers.entrySet()) {
					
					// Extract node id and node 
					long nodeId = entry.getKey();
					RestNode nodeResearcher = entry.getValue();
				
					// convert researcher node to the map
					Map<String, Object> mapResearcher = CreateResearcherMap(nodeResearcher);
					
					// query all this researcher datasets
					Map<Long, RestNode> nodesDatasets = QueryAllResearcherDatasets(nodeResearcher, null);
				
					// create flag that will signal to abort the operation
					boolean aliaseExists = false;
					
					// Query all KnwonAs Researchers
					Map<Long, RestNode> nodesKnownAsResearchers = QueryAllKnownAsNodes(nodeResearcher, null); 
					
					// Enumerate 
					for (Map.Entry<Long, RestNode> entryKnownAs : nodesKnownAsResearchers.entrySet()) {
						
						// Ignore starting Researcher
						if (entryKnownAs.getKey() != nodeId) {
							
							// If we have run in already existing researcher, abort the process						
							if (setResearchers.contains(entryKnownAs.getKey())) {
								aliaseExists = true;
								break;
							}
						
							// Get Known As Researcher node
							RestNode node = entryKnownAs.getValue();
					    
							// We will represent this researcher as one node, so copy missing data to it
							CopyNodeData(node, mapResearcher);
							
							// Add all Datasets to the node
							QueryAllResearcherDatasets(node, nodesDatasets);
						}
					}
					 
					// this researcher already has been created, abort this iteration and proceede to the nex one
					if (aliaseExists)
						continue;
					
					for (RestNode nodeDataset : nodesDatasets.values()) {
						// Create Dataset map
						Map<String, Object> mapDataset = CreateDatasetMap(nodeDataset);
						
						// Add Dataset as Researcher children
						AddChildern(mapDataset, mapResearcher);
					}
					
					// Add Researcher as Grant children
					AddChildern(mapResearcher, mapGrant);
					
					// Save added researcher node id
					setResearchers.add(nodeId);
				}
				
				// Add Grant as Institution children
				AddChildern(mapGrant, mapInstitution);
				
				if (MAX_GRANTS > 0 && ++grantCounter >= MAX_GRANTS)
					break;
			}
			
			try {
				String json = mapper.writeValueAsString(mapInstitution);
				String fileName = "json/" + Long.toString(nodeInstitution.getId()) + ".json";
				
				Writer writer = new BufferedWriter(new OutputStreamWriter(
				          new FileOutputStream(new File(fileName)), "utf-8"));
				
				writer.write(json);
				writer.close();
				
				mapNodes.put((String) nodeInstitution.getProperty(FIELD_NAME), Long.toString(nodeInstitution.getId()) + ".json");			
			} catch (JsonGenerationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonMappingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			String json = mapper.writeValueAsString(mapNodes);
			String fileName = "json/index.json";
			
			Writer writer = new BufferedWriter(new OutputStreamWriter(
			          new FileOutputStream(new File(fileName)), "utf-8"));
			
			writer.write(json);
			writer.close();
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*
	private void QueryAllInstitutionGrants(RestNode nodeInstitution, Map<String, Object> mapInstitution) {
		// Get all AdminInstitute Relationships
		Iterable<Relationship> adminInstituteGrants = nodeInstitution.getRelationships(RelTypes.AdminInstitute, Direction.INCOMING);
		if (null != adminInstituteGrants) {
			int grantCounter = 0;
			
			for (Relationship adminInstituteGrant : adminInstituteGrants) {
				// Get Grant Node
				RestNode nodeGrant = (RestNode) adminInstituteGrant.getStartNode();
				
				// Create map from Grant Node
				Map<String, Object> mapGrant = CreateGrantMap(nodeGrant);
				
				// Get all Researchers
				QueryAllGrantResearchers(nodeGrant, mapGrant);
				
				// Add Grant as Institution children
				AddChildern(mapGrant, mapInstitution);
				
				// Set limits if required
				if (MAX_GRANTS > 0 && ++grantCounter >= MAX_GRANTS)
					break;
			}
		}
	}*/
	
	
	
	/*
	
	@SuppressWarnings("unused")
	private void QueryAllGrantResearchers(RestNode nodeGrant, Map<String, Object> mapGrant) {
		Iterable<Relationship> investigatorResearchers = nodeGrant.getRelationships(RelTypes.Investigator, Direction.INCOMING);
		if (null != investigatorResearchers) {
			int researcherCounter = 0;
			
			for (Relationship investigatorResearcher : investigatorResearchers) {
				// Get Grant Node
				RestNode nodeResearcher = (RestNode) investigatorResearcher.getStartNode();
				
				// Create map from Grant Node
				Map<String, Object> mapResearcher = CreateMapFromNode(nodeResearcher, 
						 CLASS_RESEARCHER, COLOR_RESEARCHER, 1);
				
				// Get all Datasets
				
				// Get all KnownAs Relationships
				Iterable<Relationship> knownAsResearchers = nodeResearcher.getRelationships(RelTypes.KnownAs);
				// Copy data from all KnownAs Relationships
				if (null != knownAsResearchers)
					for (Relationship knownAsResearcher : knownAsResearchers) {
						RestNode nodeChildResearcher = 
						if (knownAsResearcher.getStartNode().getId() != nodeResearcher.getId()) {
							Map<String, Object> mapChildResearcher = CreateMapFromNode((RestNode) knownAsResearcher.getStartNode(), 
									 CLASS_RESEARCHER, COLOR_RESEARCHER, 1);
							
							CopyNodeData((RestNode) knownAsResearcher.getStartNode(), mapResearcher);
						} else {
							CopyNodeData((RestNode) knownAsResearcher.getStartNode(), mapResearcher);
						}
					}
				
				
				
				// Add Grant as Grant children
				AddChildern(mapResearcher, mapGrant);
				
				// Set limits if required
				if (MAX_RESEARCHERS > 0 && ++researcherCounter >= MAX_RESEARCHERS)
					break;
			}
		}
		
	}*/
	
	/*
	private Map<String, Object> CreateResearcherMap(RestNode nodeResearcher) {
		// Create map from Institution node
		Map<String, Object> mapInstitution = CreateMapFromNode(nodeResearcher, 
				 CLASS_RESEARCHER, COLOR_RESEARCHER, 1);

		// Get all KnownAs Relationships
		List<RestNode> knownAsInstitutions = QueryAllKnownAsNodes(nodeResearcher);
		if (null != knownAsInstitutions)
			for (RestNode knownAs : knownAsInstitutions)
				CopyNodeData(knownAs, mapInstitution);

		return mapInstitution;
	}
	*/
	
	/*
	private void QueryAllResearcherDataset(RestNode nodeResearcher, Map<String, Object> mapResearcher) {
		Iterable<Relationship> investigatorResearchers = nodeGrant.getRelationships(RelTypes.Investigator, Direction.INCOMING);
		if (null != investigatorResearchers) {
			int researcherCounter = 0;
			
			for (Relationship investigatorResearcher : investigatorResearchers) {
				// Get Grant Node
				RestNode nodeResearcher = (RestNode) investigatorResearcher.getStartNode();
				
				// Create map from Grant Node
				Map<String, Object> mapResearcher = CreateMapFromNode(nodeResearcher, 
						 CLASS_RESEARCHER, COLOR_RESEARCHER, 1);
				
				// Get all Datasers
				
				// Add Grant as Grant children
				AddChildern(mapResearcher, mapGrant);
				
				// Set limits if required
				if (MAX_RESEARCHERS > 0 && ++researcherCounter >= MAX_RESEARCHERS)
					break;
			}
		}
		
	}
	*/
	
	private String MakeName(final String name) {
		if (name.length() <= MAX_NAME)
			return name;
		else
			return name.substring(0, MAX_NAME - 3) + "...";
	}
	
	private Map<String, Object> CreateInstitutionMap(RestNode nodeInstitution) {
		// Create map from Institution node
		return CreateMapFromNode(nodeInstitution, 
				MakeName((String) nodeInstitution.getProperty(FIELD_NAME)),
				CLASS_INSTITUTION, COLOR_INSTITUTION, 1);
	}
	
	private Map<String, Object> CreateGrantMap(RestNode nodeGrant) {
		// Create map from Grant node
		return CreateMapFromNode(nodeGrant,
				MakeName((String) nodeGrant.getProperty(FIELD_SCIENTIFIC_TITLE)),
				CLASS_GRANT, COLOR_GRANT, 1);
	}
	
	private Map<String, Object> CreateResearcherMap(RestNode nodeResearcher) {
		// Create map from Researcher node
		return CreateMapFromNode(nodeResearcher,
				MakeName((String) nodeResearcher.getProperty(FIELD_FULL_NAME)),
				CLASS_RESEARCHER, COLOR_RESEARCHER, 1);
	}
	
	private Map<String, Object> CreateDatasetMap(RestNode nodeDataset) {
		// Create map from Researcher node
		return CreateMapFromNode(nodeDataset, 
				MakeName(Long.toString(nodeDataset.getId())), 
				CLASS_DATASET, COLOR_DATASET, 1);
	}
	
	private Map<String, Object> CreateMapFromNode(final RestNode node, 
			final String name, final String strClass, final String strColor, int size)
	{
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(PROPERTY_SELF, node.getUri());
		map.put(PROPERTY_NAME, name);
		map.put(PROPERTY_CLASS, strClass);
		map.put(PROPERTY_COLOR, strColor);
		map.put(PROPERTY_SIZE, size);
		//map.put(PROPERTY_X, 0);
		//map.put(PROPERTY_Y, 0);
		
		CopyNodeData(node, map);
		
		return map;
	}
	
	private void CopyNodeData(final RestNode node, Map<String, Object> map) {
		if (!map.containsKey(PROPERTY_DATA))
			map.put(PROPERTY_DATA, new HashMap<String, Object>());
		
		Iterable<String> keys = node.getPropertyKeys();
		@SuppressWarnings("unchecked")
		Map<String, Object> data = (Map<String, Object>) map.get(PROPERTY_DATA);
		for (String key : keys) 
			if (!data.containsKey(key)) 
				data.put(key, node.getProperty(key));
	}
	
	private void AddChildern(Object children, Map<String, Object> map) {
		if (!map.containsKey(PROPERTY_CHILDREN))
			map.put(PROPERTY_CHILDREN, new ArrayList<Object>());
		
		@SuppressWarnings("unchecked")
		List<Object> childrens = (List<Object>) map.get(PROPERTY_CHILDREN);
		childrens.add(children);
	}
	
	private Map<Long, RestNode> QueryAllKnownAsNodes(RestNode node, Map<Long, RestNode> nodes) {
		// Make sure we have map created
		if (null == nodes) 
			nodes = new HashMap<Long, RestNode>();
		
		// Check what we does not have this node in the map
		if (!nodes.containsKey(node.getId())) {
		
			// Add node to the map, so we will not query it again
			nodes.put(node.getId(), node);
		
			// Get all KnownAs Relationships
			Iterable<Relationship> knownAsRelationships = node.getRelationships(RelTypes.KnownAs);
			// Add new nodes to the map and get it relationships as well
			if (null != knownAsRelationships) 
				for (Relationship knownAs : knownAsRelationships) {
					QueryAllKnownAsNodes((RestNode) knownAs.getStartNode(), nodes);
					QueryAllKnownAsNodes((RestNode) knownAs.getEndNode(), nodes);
				}
		}
		
		return nodes;
	}	
	
	private Map<Long, RestNode> QueryAllInstitutionGrants(RestNode nodeInstitution, 
			Map<Long, RestNode> nodesGrants) {
		// Make sure we have map created
		if (null == nodesGrants) 
			nodesGrants = new HashMap<Long, RestNode>();
		
		// Query all relationships 
		Iterable<Relationship> relGrants = nodeInstitution.getRelationships(RelTypes.AdminInstitute, Direction.INCOMING);
		if (null != relGrants) 
			for (Relationship relGrant : relGrants) {
				
				// Extract only start node because we know direction
				RestNode nodeGrant = (RestNode) relGrant.getStartNode();
				
				// Check what we does not have this Grant in the map and add it if not.
				if (!nodesGrants.containsKey(nodeGrant.getId())) 
					nodesGrants.put(nodeGrant.getId(), nodeGrant);				
			}
		
		// return the map
		return nodesGrants;
	}
	
	private Map<Long, RestNode> QueryAllGrantResearchers(RestNode nodeGrant, 
			Map<Long, RestNode> nodesResearchers) {
		// Make sure we have map created
		if (null == nodesResearchers) 
			nodesResearchers = new HashMap<Long, RestNode>();
		
		// Query all relationships 
		Iterable<Relationship> relResearchers = nodeGrant.getRelationships(RelTypes.Investigator, Direction.INCOMING);
		if (null != relResearchers) 
			for (Relationship relResearcher : relResearchers) {
				
				// Extract only start node because we know direction
				RestNode nodeResearcher = (RestNode) relResearcher.getStartNode();
				
				// Check what we does not have this Grant in the map and add it if not.
				if (!nodesResearchers.containsKey(nodeResearcher.getId())) 
					nodesResearchers.put(nodeResearcher.getId(), nodeResearcher);				
			}
		
		// return the map
		return nodesResearchers;
	}
	
	private Map<Long, RestNode> QueryAllResearcherDatasets(RestNode nodeResearcher, 
			Map<Long, RestNode> nodesDatasets) {
		// Make sure we have map created
		if (null == nodesDatasets) 
			nodesDatasets = new HashMap<Long, RestNode>();
		
		// Query all relationships 
		Iterable<Relationship> relDatasets = nodeResearcher.getRelationships(RelTypes.RelatedTo, Direction.OUTGOING);
		if (null != relDatasets) 
			for (Relationship relDataset : relDatasets) {
				
				// Extract only start node because we know direction
				RestNode nodeDataset = (RestNode) relDataset.getEndNode();
				
				// Check what we does not have this Grant in the map and add it if not.
				if (!nodesDatasets.containsKey(nodeDataset.getId())) 
					nodesDatasets.put(nodeDataset.getId(), nodeDataset);				
			}
		
		// return the map
		return nodesDatasets;
	}
}
