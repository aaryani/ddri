package org.vsc.harvesters.rda;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.index.RestIndex;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class Harvester {
	private static final int MAX_ROWS = 200;
	private static final String BASE_URL = "https://demo.ands.org.au/registry/services/api/registry_objects/";
	private static final String LIST_OBJECTS_URL = BASE_URL + "?rows=%d&fl=id&start=%d";
	private static final String GET_OBJECT_URL = BASE_URL + "%s";
	private static final String GET_RELATIONSHIP_URL = BASE_URL + "%s/relationships";
	
	
	private static final String FILED_STATUS = "status";
	private static final String FILED_MESSAGE = "message";
	private static final String FILED_ERROR = "error";
	private static final String FIELD_MSG = "msg";
	private static final String FIELD_TRACE = "trace";
	private static final String FILED_RESPONSE = "response";
	private static final String FILED_REGISTRY_OBJECT = "registry_object";
	private static final String FIELD_RELATIONSHIPS = "relationships";
	
	private static final String STATUS_SUCCESS ="success";
	
	private static final String PART_COUNT = "_count";
		
	private static final ObjectMapper mapper = new ObjectMapper();   
	private static final TypeReference<LinkedHashMap<String, Object>> linkedHashMapTypeReference = new TypeReference<LinkedHashMap<String, Object>>() {};   

	private RestAPI graphDb;
	private RestCypherQueryEngine engine;
	private RestIndex<Node> index;
	private Set<String> objectIds;
	
	private Label labelRecord = DynamicLabel.label(Record.LABEL_RECORD);
	private Label labelRDA = DynamicLabel.label(Record.LABEL_RDA);
	
	public Harvester( final String neo4jUrl ) {
		
		graphDb = new RestAPIFacade(neo4jUrl); //"http://localhost:7474/db/data/");  
		engine = new RestCypherQueryEngine(graphDb);  
		
		engine.query("CREATE CONSTRAINT ON (n:" + Record.LABEL_RDA_RECORD + ") ASSERT n." + Record.PROPERTY_RDA_ID + " IS UNIQUE", Collections.<String, Object> emptyMap());

		index = graphDb.index().forNodes(Record.LABEL_RDA_RECORD);
		
		objectIds = new HashSet<String>();
	}	
	
	public void harvest() throws Exception {
		getObjects();
		getRelationships();
	}
	
	@SuppressWarnings("unchecked")
	private void getObjects() throws Exception {
		int numFound = 0; 
		int from = 0;
				
		do 
		{
			Map<String, Object> response = (Map<String, Object>) getNestedObject(listObjects(from, MAX_ROWS), FILED_RESPONSE);
			if (null != response) {
				RecordSet recordSet = RecordSet.fromJson(response);
				if (null != recordSet && recordSet.processed > 0) {
					if (0 == numFound)
						numFound = recordSet.found;
							
					for (String recordId : recordSet.recordIds) {
						Map<String, Object> registryObject = (Map<String, Object>) getNestedObject(getObject(recordId), FILED_REGISTRY_OBJECT);
						if (null != registryObject) { 
							objectIds.add(recordId);
											
							createRecord(recordId, Record.fromJson(registryObject));
						}
					}
						
					from += recordSet.processed;
				} else 
					break;
			}
		} while (from < numFound);
	}
	
	@SuppressWarnings("unchecked")
	private void getRelationships() throws Exception {
		for (String objectId : objectIds) {
			List<Map<String, Object>> relationships = (List<Map<String, Object>>) getNestedObject(getRelationship(objectId), FIELD_RELATIONSHIPS);
			if (null != relationships) {
				
				RestNode node = getNodeById(objectId);
				if (null != node) 
					for (Map<String, Object> relationship : relationships) 
						for (Map.Entry<String, Object> entry : relationship.entrySet()) 
							if (!entry.getKey().contains(PART_COUNT)) {
								List<Map<String, Object>> list = (List<Map<String, Object>>) entry.getValue();
								for (Map<String, Object> item : list) 
									createRelationship(node, org.vsc.harvesters.rda.Relationship.fromJson(item, entry.getKey()));								
							}
						
					
				
			}
		}
	}
	
	RestNode getNodeById(final String ObjectId) {
		IndexHits<Node> hits = index.get(Record.PROPERTY_RDA_ID, ObjectId);
		return (RestNode) hits.getSingle();
	}

	private Map<String, Object> listObjects( final int from, final int size) {
		for (int i = 0; i < 10; ++i) {
			try {
				Map<String, Object> json = get(String.format(LIST_OBJECTS_URL, size, from));
				if (null != json)
					return json;
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}
	
	private Map<String, Object> getObject( final String id) {
		
		for (int i = 0; i < 10; ++i) {
			try {
				Map<String, Object> json = get(String.format(GET_OBJECT_URL, id));
				if (null != json)
					return json;
			} catch (Exception e) {
				e.printStackTrace();
			}		
			
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}			
		}
		
		return null;
	}
	
	private Map<String, Object> getRelationship( final String id) {
		
		for (int i = 0; i < 10; ++i) {
			try {
				Map<String, Object> json = get(String.format(GET_RELATIONSHIP_URL, id));
				if (null != json)
					return json;
			} catch (Exception e) {
				e.printStackTrace();
			}		
			
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}
	
	@SuppressWarnings("unchecked")
	private Object getNestedObject(Map<String, Object> json, final String propertyName) throws Exception {
		if (null != json) {
			String status = (String) json.get(FILED_STATUS);
			if (null != status && status.equals(STATUS_SUCCESS)) {
				Map<String, Object> message = (Map<String, Object>) json.get(FILED_MESSAGE);
				if (null != message) {
					Map<String, Object> error = (Map<String, Object>) message.get(FILED_ERROR);
					if (null != error) 
						processError((String) error.get(FIELD_MSG), (String) error.get(FIELD_TRACE));
					else
						return message.get(propertyName);
				}
				else
					throw new Exception("Invalid response format, unable to find message data");
			} else
				throw new Exception("Invalid response status");
		} else
			throw new Exception("Invalid response");
		
		return null;
	}
	
	private void processError( final String msg, final String trace) throws Exception {
		throw new Exception ("Error: " + msg + ", Trace: " + trace);
	}
		
	private Map<String, Object> get( final String url ) {
		System.out.println("Downloading: " + url);
				
		ClientResponse response = Client.create()
								  .resource( url )
								  .accept( MediaType.APPLICATION_JSON ) 
								  .type( MediaType.APPLICATION_JSON )
								  .get( ClientResponse.class );
		
		try {
			return mapper.readValue( response.getEntity( String.class ), linkedHashMapTypeReference);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
    }   
	
	private void createRecord(final String id, Record record) {
		System.out.println("Create Record: " + record.toString());
			
		RestNode node = graphDb.getOrCreateNode(index, Record.PROPERTY_RDA_ID, id, record.data);
		if (!node.hasLabel(labelRecord))
			node.addLabel(labelRecord); 
		if (!node.hasLabel(labelRDA))
			node.addLabel(labelRDA);
	}
	
	private void createRelationship(RestNode nodeFrom, org.vsc.harvesters.rda.Relationship relationship ) {
		System.out.println("Create Record: " + relationship.toString());
		
		RestNode nodeTo = getNodeById(relationship.relatedObjectId);
		if (null != nodeTo) 
			createUniqueRelationship(nodeFrom, nodeTo, 
					DynamicRelationshipType.withName(relationship.relationsipType), relationship.data);
	}
	
	private void createUniqueRelationship(RestNode nodeStart, RestNode nodeEnd, 
			RelationshipType type, Map<String, Object> data) {

		// get all node relationships. They should be empty for a new node
		Iterable<Relationship> rels = nodeStart.getRelationships(type);		
		for (Relationship rel : rels) 
			if (rel.getStartNode().getId() == nodeStart.getId() && 
				rel.getEndNode().getId() == nodeEnd.getId())
				return;
		
		graphDb.createRelationship(nodeStart, nodeEnd, type, data);
	}	
	
}
