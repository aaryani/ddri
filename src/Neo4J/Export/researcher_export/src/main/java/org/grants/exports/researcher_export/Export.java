package org.grants.exports.researcher_export;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;
import org.neo4j.rest.graphdb.util.Config;
import org.neo4j.rest.graphdb.util.QueryResult;

public class Export {
	
	private static final String FIELD_DOI = "doi";
	private static final String FIELD_NAME = "name";
	private static final String FIELD_SCIENTIFIC_TITLE = "scientific_title";
	private static final String FIELD_FULL_NAME = "full_name";
	
	private static enum RelTypes implements RelationshipType
    {
    	AdminInstitute, Investigator, KnownAs, RelatedTo
    }
	
	//MATCH (n:NHMRC:Researcher) RETURN n LIMIT 50 UNION MATCH (n:ARC:Researcher) RETURN n LIMIT 25 UNION MATCH (n:Web:Researcher) RETURN n
	
	// Qyery all instutuions ID:
	// MATCH (n:NHMRC:Institution) RETURN id(n) UNION MATCH (n:ARC:Institution) RETURN id(n) 
	
	public void ExportResearchers(final String serverRoot)
	{
		new File("json").mkdirs();
		
		System.setProperty(Config.CONFIG_STREAM, "true");
		
		// connect to graph database
		RestAPI graphDb = new RestAPIFacade(serverRoot);  
		
		// Create cypher engine
		RestCypherQueryEngine engine=new RestCypherQueryEngine(graphDb);  
				
		// create object mapper
		ObjectMapper mapper = new ObjectMapper();   
			
		// create map to store result;
		Map<String, String> mapNodes = new HashMap<String, String>();
		
		// make sure we have an index on ARC_Grant:arc_project_id
		// RestAPI does not supported indexes created by schema, so we will use Cypher for that
		QueryResult<Map<String, Object>> researchers = engine.query("MATCH (n:NHMRC:Researcher) RETURN n LIMIT 50 UNION MATCH (n:ARC:Researcher) RETURN n LIMIT 25 UNION MATCH (n:Web:Researcher) RETURN n", null);
		for (Map<String, Object> row : researchers) {
			RestNode nodeResearcher = (RestNode) row.get("n");
		
			if (null == nodeResearcher) {
				System.out.println("Invalid node");
				break;
			}
			
			// extract node id
			long researcherNodeId = nodeResearcher.getId();
			// extract node name
			String researcherName = (String) nodeResearcher.getProperty(FIELD_FULL_NAME);
			
			System.out.println("Processing researcher: " + researcherName + " (" + researcherNodeId + ")");
			
			// Create Dataset 
			CompiledNode researcher = CreateResearcher(nodeResearcher);

			// qyery all known as researchers
			// NHMRC, ARC and Web researchers aren't connected in current version of the system, so this should be safe 
			QueryAllKnownAsNodes(nodeResearcher, researcher);
			
			// Query all researcher datatses
			List<CompiledNode> datatsets = QueryAllResearchersDatasets(researcher);
			
			// Now enumerate datasets 
			if (null != datatsets)
				for (CompiledNode datatset : datatsets) 
					researcher.AddChildern(datatset);
					
			// query all this researcher grants
			List<CompiledNode> grants = QueryAllResearcherGrants(researcher);
					
			// now enumerate the grants
			if (null != grants)
				for (CompiledNode grant : grants) {
						
					// query all this grants institutions
					List<CompiledNode> insitutions = QueryAllGrantInstitutions(grant);
					
					// now enumerate the institutions
					if (null != insitutions)
						for (CompiledNode institution : insitutions) 
							grant.AddChildern(institution);					
								
					researcher.AddChildern(grant);
				}
					
			try {
				String json = mapper.writeValueAsString(researcher.getData());
				String fileName = Long.toString(researcherNodeId) + ".json";
				
				Writer writer = new BufferedWriter(new OutputStreamWriter(
				          new FileOutputStream(new File("json/" + fileName)), "utf-8"));
				
				writer.write(json);
				writer.close();
				
				mapNodes.put(researcherName, fileName);			
			} catch (JsonGenerationException e) {
				e.printStackTrace();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		try {
			
			Map<String, Object> mapIndex = new HashMap<String, Object>();
			mapIndex.put("select", "Researcher: ");
			mapIndex.put("comments", "Only 50 HNMRC, 25 ARC and 25 Web Researcher has been selected. Click on node to view it's parameters. Double click on it to collapse or expand.");
			mapIndex.put("index", mapNodes);
			mapIndex.put("legend", CompiledNode.getLegend());
			
			String json = mapper.writeValueAsString(mapIndex);
			String fileName = "json/index.json";
			
			Writer writer = new BufferedWriter(new OutputStreamWriter(
			          new FileOutputStream(new File(fileName)), "utf-8"));
			
			writer.write(json);
			writer.close();
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private CompiledNode CreateInstitution(RestNode node) {
		return new CompiledNode(node, 
				CompiledNode.NodeType.Institution, 
				(String) node.getProperty(FIELD_NAME));
	}
	
	private CompiledNode CreateGrant(RestNode node) {
		return new CompiledNode(node, 
				CompiledNode.NodeType.Grant, 
				(String) node.getProperty(FIELD_SCIENTIFIC_TITLE));
	}
	
	private CompiledNode CreateResearcher(RestNode node) {
		return new CompiledNode(node, 
				CompiledNode.NodeType.Researcher, 
				(String) node.getProperty(FIELD_FULL_NAME));
	}
	
	private CompiledNode CreateDataset(RestNode node) {
		return new CompiledNode(node, 
				CompiledNode.NodeType.Dataset, 
				(String) node.getProperty(FIELD_DOI));
	}
	

	private void QueryAllKnownAsNodes(RestNode node, CompiledNode compiledNode) {
		// Get all KnownAs Relationships
		Iterable<Relationship> rels = node.getRelationships(RelTypes.KnownAs);
		if (null != rels) 
			for (Relationship rel : rels) {
				// Extract start and end node
				RestNode startNode = (RestNode) rel.getStartNode();
				if (!compiledNode.isNodeExists(startNode.getId())) {
					compiledNode.addRestNode(startNode);						
					QueryAllKnownAsNodes(startNode, compiledNode);
				}
				
				RestNode endNode = (RestNode) rel.getEndNode();
				if (!compiledNode.isNodeExists(endNode.getId())) {
					compiledNode.addRestNode(endNode);						
					QueryAllKnownAsNodes(endNode, compiledNode);
				}
			}
	}		
	
	private List<CompiledNode> QueryAllDatasetResearchers(CompiledNode dataset) {
		List<CompiledNode> researchers = null;
		
		for (RestNode nodeDataset : dataset.getNodes()) {
			Iterable<Relationship> relResearchers = nodeDataset.getRelationships(
					RelTypes.RelatedTo, Direction.INCOMING);
			
			if (null != relResearchers) 
				for (Relationship relResearcher : relResearchers) {
					
					// Extract only start node because we know direction
					RestNode nodeResearcher = (RestNode) relResearcher.getStartNode();
					
					// create list if needed
					if (null == researchers)
						researchers = new ArrayList<CompiledNode>();
					
					// check what we do not have this researcher
					CompiledNode researcher = CompiledNode.findNodeInList(researchers, nodeResearcher.getId());
					if (null == researcher)
					{
						// create new researcher node
						researcher = CreateResearcher(nodeResearcher);
						
						// Query all known as researchers
						QueryAllKnownAsNodes(nodeResearcher, researcher);
						
						// add it to the list
						researchers.add(researcher);
					}
				}			
		} 
		
		return researchers;
	}
	
	private List<CompiledNode> QueryAllResearcherGrants(CompiledNode researcher) {
		List<CompiledNode> grants = null;
		
		for (RestNode nodeResearcher : researcher.getNodes()) {
			Iterable<Relationship> relGrants = nodeResearcher.getRelationships(
					RelTypes.Investigator, Direction.OUTGOING);
			
			if (null != relGrants) 
				for (Relationship relGrant : relGrants) {
					
					// Extract only end node because we know direction
					RestNode nodeGrant = (RestNode) relGrant.getEndNode();
					
					// create list if needed
					if (null == grants)
						grants = new ArrayList<CompiledNode>();
					
					// check what we do not have this researcher
					CompiledNode grant = CompiledNode.findNodeInList(grants, nodeGrant.getId());
					if (null == grant)
					{
						// add it to the list
						grants.add(CreateGrant(nodeGrant));
					}
				}	
		}
		
		return grants;
	}
	
	private List<CompiledNode> QueryAllResearchersDatasets(CompiledNode researcher) {
		List<CompiledNode> datasets = null;
		
		for (RestNode nodeResearcher : researcher.getNodes()) {
			Iterable<Relationship> relDatatsets = nodeResearcher.getRelationships(
					RelTypes.RelatedTo, Direction.OUTGOING);
			
			if (null != relDatatsets) 
				for (Relationship relDatatset : relDatatsets) {
					
					// Extract only end node because we know direction
					RestNode nodeDataset = (RestNode) relDatatset.getEndNode();
					
					// create list if needed
					if (null == datasets)
						datasets = new ArrayList<CompiledNode>();
					
					// check what we do not have this researcher
					CompiledNode dataset = CompiledNode.findNodeInList(datasets, nodeDataset.getId());
					if (null == dataset)
					{
						// create new researcher node
						dataset = CreateDataset(nodeDataset);
						
						// Query all known as researchers
						QueryAllKnownAsNodes(nodeDataset, dataset);
						
						// add it to the list
						datasets.add(dataset);					}
				}	
		}
		
		return datasets;
	}
	
	private List<CompiledNode> QueryAllGrantInstitutions(CompiledNode grant) {
		List<CompiledNode> insitutions = null;
		
		for (RestNode nodeGrant : grant.getNodes()) {
			Iterable<Relationship> relInsitutions = nodeGrant.getRelationships(
					RelTypes.AdminInstitute, Direction.OUTGOING);
			
			if (null != relInsitutions) 
				for (Relationship relInsitution : relInsitutions) {
					
					// Extract only end node because we know direction
					RestNode nodeInstitution = (RestNode) relInsitution.getEndNode();
					
					// create list if needed
					if (null == insitutions)
						insitutions = new ArrayList<CompiledNode>();
					
					// check what we do not have this researcher
					CompiledNode insitution = CompiledNode.findNodeInList(insitutions, nodeInstitution.getId());
					if (null == insitution)
					{
						// create new institution node
						insitution = CreateInstitution(nodeInstitution);
						
						// Query all known as insitutions
						QueryAllKnownAsNodes(nodeInstitution, insitution);
						
						// add it to the list
						insitutions.add(insitution);
					}
				}	
		}
		
		return insitutions;
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
	/*
	
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
	
	*/
}
