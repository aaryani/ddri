package org.grants.compiler;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.grants.crossref.Author;
import org.grants.crossref.CrossRef;
import org.grants.crossref.Item;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.index.RestIndex;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;
import org.neo4j.rest.graphdb.util.QueryResult;

public class Compiler {
	private static final String NEO4J1_URI = "http://localhost:7474/db/data/";
	private static final String NEO4J2_URI = "http://localhost:7476/db/data/";
	
	private static final String LABEL_RECORD = "Record";
	private static final String LABEL_PUBLICATION = "Publication";
	private static final String LABEL_RESEARCHER = "Researcher";
	
	private static final String LABEL_RDA = "RDA";
	
	private static final String LABEL_DRYAD = "Dryad";
	private static final String LABEL_DRYAD_PUBLICATION = LABEL_DRYAD + "_" + LABEL_PUBLICATION;
	
	private static final String LABEL_CROSSREF = "CrossRef";
	private static final String LABEL_CROSSREF_PUBLICATION = LABEL_CROSSREF + "_" + LABEL_PUBLICATION;
	private static final String LABEL_CROSSREF_RESEARCHER = LABEL_CROSSREF + "_" + LABEL_RESEARCHER;
	
	private static final String RELATIONSHIP_AUTHOR = "author";
	private static final String RELATIONSHIP_EDITOR = "editor";
	private static final String RELATIONSHIP_KNOWN_AS = "knownAs";
		
	private static final String PROPERTY_KEY = "key";
	private static final String PROPERTY_OAI = "oai";
	private static final String PROPERTY_RDA_ID = "rda_id";
	private static final String PROPERTY_NODE_TYPE = "node_type";
	private static final String PROPERTY_NODE_SOURCE = "node_source";
	private static final String PROPERTY_IS_REFERENCED_BY = "isReferencedBy";
	private static final String PROPERTY_URL = "url";
	private static final String PROPERTY_PREFIX = "url";
	private static final String PROPERTY_ISSUE = "issue";
	private static final String PROPERTY_VOLUME = "volume";
	private static final String PROPERTY_TYPE = "type";
	private static final String PROPERTY_PAGE = "page";
	private static final String PROPERTY_PUBLISHER = "publisher";
	private static final String PROPERTY_ISSN = "issn";
	private static final String PROPERTY_TITLE = "title";
	private static final String PROPERTY_SUBTITLE = "subtitle";
	private static final String PROPERTY_SUBJECT = "subject";
	private static final String PROPERTY_CONTAINER_TITLE = "title_container";
	private static final String PROPERTY_AUTHOR = "author";
	private static final String PROPERTY_EDITOR = "editor";
	private static final String PROPERTY_ISSUED = "issued";
	private static final String PROPERTY_DEPOSITED = "deposited";
	private static final String PROPERTY_INDEXED = "indexed";
	private static final String PROPERTY_SCORE = "score";
	private static final String PROPERTY_UPDATE_POLICY = "update_policy";
	private static final String PROPERTY_SUFFIX = "suffix";
	private static final String PROPERTY_FAMILY_NAME = "family_name";
	private static final String PROPERTY_GIVEN_NAME = "given_name";
	private static final String PROPERTY_FULL_NAME = "full_name";
	private static final String PROPERTY_ORCID = "orcid";

	private static final String PART_DOI = "doi:";
	
	private static Label labelPublication = DynamicLabel.label(LABEL_PUBLICATION);
	private static Label labelResearcher = DynamicLabel.label(LABEL_RESEARCHER);
	private static Label labelRDA = DynamicLabel.label(LABEL_RDA);
	private static Label labelDryad = DynamicLabel.label(LABEL_DRYAD);
	private static Label labelCrossRef = DynamicLabel.label(LABEL_CROSSREF);

	private static RelationshipType relAuthor = DynamicRelationshipType.withName(RELATIONSHIP_AUTHOR);
	private static RelationshipType relEditor = DynamicRelationshipType.withName(RELATIONSHIP_EDITOR);
	private static RelationshipType relKnownAs = DynamicRelationshipType.withName(RELATIONSHIP_KNOWN_AS);
	
	private RestAPI graphDb1;
	private RestAPI graphDb2;
	
	private RestCypherQueryEngine engine1;  
	private RestCypherQueryEngine engine2;  
	
	private RestIndex<Node> indexDryadPublication;
	private RestIndex<Node> indexCrossrefPublication;
	private RestIndex<Node> indexCrossrefResearcher;
	
	CrossRef crossref = new CrossRef();
	
	public Compiler() {
		// connect to graph database
		graphDb1 = new RestAPIFacade(NEO4J1_URI);  
		graphDb2 = new RestAPIFacade(NEO4J2_URI);  
				
		// Create cypher engine
		engine1 = new RestCypherQueryEngine(graphDb1);  
		engine2 = new RestCypherQueryEngine(graphDb2);  
		
		// create constraints
		engine2.query("CREATE CONSTRAINT ON (n:" + LABEL_DRYAD_PUBLICATION + ") ASSERT n." + PROPERTY_KEY + " IS UNIQUE", Collections.<String, Object> emptyMap());
		engine2.query("CREATE CONSTRAINT ON (n:" + LABEL_CROSSREF_PUBLICATION + ") ASSERT n." + PROPERTY_KEY + " IS UNIQUE", Collections.<String, Object> emptyMap());
		engine2.query("CREATE CONSTRAINT ON (n:" + LABEL_CROSSREF_RESEARCHER + ") ASSERT n." + PROPERTY_KEY + " IS UNIQUE", Collections.<String, Object> emptyMap());

		// get indexes
		indexDryadPublication = graphDb2.index().forNodes(LABEL_DRYAD_PUBLICATION);
		indexCrossrefPublication = graphDb2.index().forNodes(LABEL_CROSSREF_PUBLICATION);
		indexCrossrefResearcher = graphDb2.index().forNodes(LABEL_CROSSREF_RESEARCHER);
	}
	
	@SuppressWarnings("unchecked")
	public void process() {
		
		QueryResult<Map<String, Object>> articles = engine1.query("MATCH (n:Dryad:Record) WHERE n.genre='Article' RETURN n", null);
		for (Map<String, Object> row : articles) {
			RestNode nodeRecord = (RestNode) row.get("n");
		
			if (null == nodeRecord) {
				System.out.println("Invalid node");
				break;
			}
			
			Map<String, Object> props = getProperties(nodeRecord);
			
			String key = (String) props.get(PROPERTY_OAI);
			if (null == key || key.isEmpty()) {
				System.out.println("Invalid node key");
				break;
			}
			
			System.out.println(key);
			
			RestNode nodePublication = createNode(graphDb2, indexDryadPublication, PROPERTY_KEY, key, 
					labelPublication, labelDryad, props);
			if (null == nodePublication) {
				System.out.println("Unable to create publication");
				break;
			}
			
			Object referencedBy = props.get(PROPERTY_IS_REFERENCED_BY);
			if (null != referencedBy) {
				if (referencedBy instanceof String) {
					RestNode node = createCrossRefPublication((String) referencedBy);
					if (null != node)
						createUniqueRelationship(graphDb2, nodePublication, node, relKnownAs, Direction.BOTH, null);
				} else if (referencedBy instanceof String[]) {
					for (String doi :(String[]) referencedBy) {
						RestNode node = createCrossRefPublication((String) doi);
						if (null != node)
							createUniqueRelationship(graphDb2, nodePublication, node, relKnownAs, Direction.BOTH, null);
					}
				} else if (referencedBy instanceof List<?>) {
					for (String doi :(List<String>) referencedBy) {
						RestNode node = createCrossRefPublication((String) doi);
						if (null != node)
							createUniqueRelationship(graphDb2, nodePublication, node, relKnownAs, Direction.BOTH, null);
					}
				}
					
			}
		}
	}
	
	private RestNode createCrossRefPublication(final String key) {
		if (key.contains(PART_DOI)) {
			String doi = key.substring(PART_DOI.length());
			Item item = crossref.requestWork(doi);
			if (null != item) {
				Map<String, Object> props = new HashMap<String, Object>();
				addProperty(props, PROPERTY_URL, item.getUrl());
				addProperty(props, PROPERTY_PREFIX, item.getPrefix());
				addProperty(props, PROPERTY_ISSUE, item.getIssue());
				addProperty(props, PROPERTY_VOLUME, item.getVolume());
				addProperty(props, PROPERTY_TYPE, item.getType());
				addProperty(props, PROPERTY_PAGE, item.getPage());
				addProperty(props, PROPERTY_PUBLISHER, item.getPublisher());
				addProperty(props, PROPERTY_PUBLISHER, item.getPublisher());
				addProperty(props, PROPERTY_ISSN, item.getIssn());
				addProperty(props, PROPERTY_TITLE, item.getTitle());
				addProperty(props, PROPERTY_SUBTITLE, item.getSubtitle());
				addProperty(props, PROPERTY_SUBJECT, item.getSubject());
				addProperty(props, PROPERTY_CONTAINER_TITLE, item.getContainerTitle());
				addProperty(props, PROPERTY_AUTHOR, item.getAuthorString());
				addProperty(props, PROPERTY_EDITOR, item.getEditorString());
				addProperty(props, PROPERTY_ISSUED, item.getIssuedString());
				addProperty(props, PROPERTY_DEPOSITED, item.getDepositedString());
				addProperty(props, PROPERTY_INDEXED, item.getIndexedString());
				addProperty(props, PROPERTY_SCORE, String.valueOf(item.getScore()));
				addProperty(props, PROPERTY_UPDATE_POLICY, item.getUpdatePolicy());
				
				if (!props.isEmpty()) {
					RestNode node =  createNode(graphDb2, indexCrossrefPublication, PROPERTY_KEY, doi,
							labelPublication, labelCrossRef, props); 
					
					if (null != item.getAuthor())
						for (Author author : item.getAuthor()) {
							RestNode nodeAuthor = createCrossRefResearcher(doi, author);
							
							createUniqueRelationship(graphDb2, nodeAuthor, node, relAuthor, Direction.OUTGOING, null);
						}
					
					if (null != item.getEditor())
						for (Author editor : item.getEditor()) {
							RestNode nodeAuthor = createCrossRefResearcher(doi, editor);
							
							createUniqueRelationship(graphDb2, nodeAuthor, node, relEditor, Direction.OUTGOING, null);
						}
					
					return node;
				}
			}
		}
			
		return null;
	}
	
	private RestNode createCrossRefResearcher(final String doi, Author author) {
		Map<String, Object> props = new HashMap<String, Object>();
		addProperty(props, PROPERTY_SUFFIX, author.getSuffix());
		addProperty(props, PROPERTY_GIVEN_NAME, author.getGiven());
		addProperty(props, PROPERTY_FAMILY_NAME, author.getFamily());
		addProperty(props, PROPERTY_FULL_NAME, author.getFullName());
		addProperty(props, PROPERTY_ORCID, author.getOrcid());
		
		return createNode(graphDb2, indexCrossrefResearcher, PROPERTY_KEY, doi + ":" + author.getFullName(), 
				labelResearcher, labelCrossRef, props); 

	}
	
	private void addProperty(Map<String, Object> map, final String key, final Object value) {
		if (null != key && !key.isEmpty() && null != value) {
			if (value instanceof String && ((String) value).isEmpty())
				return;
			
			map.put(key, value);				
		}
	}
	
	private Map<String, Object> getProperties(RestNode node) {
		Iterable<String> keys = node.getPropertyKeys();
		Map<String, Object> pars = null;
		
		for (String key : keys) {
			if (null == pars)
				pars = new HashMap<String, Object>();
			
			pars.put(key, node.getProperty(key));
		}
		
		return pars;
	}
	
	private RestNode createNode(RestAPI graphDb, RestIndex<Node> index, final String key, final Object value,
			final Label labelType, final Label labelSorce, Map<String, Object> props) {
				
	//	System.out.println("Creating new node " +  labelSorce.name() + ":" + labelType.name() + " " + key + "=" + value + " " + props);
		
		props.put(key, value);
		props.put(PROPERTY_NODE_TYPE, labelType.name());
		props.put(PROPERTY_NODE_SOURCE, labelSorce.name());
		
		RestNode node = graphDb.getOrCreateNode(index, key, value, props);
		if (!node.hasLabel(labelSorce))
			node.addLabel(labelSorce); 
		if (!node.hasLabel(labelType))
			node.addLabel(labelType);
		
		return node;
	}
	
	private void createUniqueRelationship(RestAPI graphDb, RestNode nodeStart, RestNode nodeEnd, 
			RelationshipType type, Direction direction, Map<String, Object> data) {

		// get all node relationships. They should be empty for a new node
		Iterable<Relationship> rels = nodeStart.getRelationships(type, direction);		
		for (Relationship rel : rels) {
			switch (direction) {
			case INCOMING:
				if (rel.getStartNode().getId() == nodeEnd.getId())
					return;
			case OUTGOING:
				if (rel.getEndNode().getId() == nodeEnd.getId())
					return;				
			case BOTH:
				if (rel.getStartNode().getId() == nodeEnd.getId() || 
				    rel.getEndNode().getId() == nodeEnd.getId())
					return;
			}
		}
		
		if (direction == Direction.INCOMING)
			graphDb.createRelationship(nodeEnd, nodeStart, type, data);
		else
			graphDb.createRelationship(nodeStart, nodeEnd, type, data);
	}	
	
	/*
	private void createUniqueRelationship(RestNode nodeStart, RestNode nodeEnd, 
			RelationshipType type, Direction direction, Map<String, Object> data) {

		// get all node relationships. They should be empty for a new node
		Iterable<Relationship> rels = nodeStart.getRelationships(type, direction);
		for (Relationship rel : rels) {
			switch ()
			
			if (rel.getStartNode().getId() == nodeStart.getId() && 
				rel.getEndNode().getId() == nodeEnd.getId())
				return;
		}
		
		graphDb.createRelationship(nodeStart, nodeEnd, type, data);
	}	*/
}
