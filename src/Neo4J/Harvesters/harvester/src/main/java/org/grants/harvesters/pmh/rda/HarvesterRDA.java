package org.grants.harvesters.pmh.rda;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.grants.harvesters.pmh.HarvesterPmh;
import org.grants.harvesters.pmh.registry.Description;
import org.grants.harvesters.pmh.registry.Electronic;
import org.grants.harvesters.pmh.registry.Identifier;
import org.grants.harvesters.pmh.registry.Location;
import org.grants.harvesters.pmh.registry.Name;
import org.grants.harvesters.pmh.registry.OriginatingSource;
import org.grants.harvesters.pmh.registry.Phisical;
import org.grants.harvesters.pmh.registry.Record;
import org.grants.harvesters.pmh.registry.RegistryActivity;
import org.grants.harvesters.pmh.registry.RegistryCollection;
import org.grants.harvesters.pmh.registry.RegistryObject;
import org.grants.harvesters.pmh.registry.RegistryParty;
import org.grants.harvesters.pmh.registry.RegistryService;
import org.grants.harvesters.pmh.registry.RelatedInfo;
import org.grants.harvesters.pmh.registry.RelatedObject;
import org.grants.harvesters.pmh.registry.Subject;
import org.grants.harvesters.pmh.registry.RegistryActivity.Type;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.index.RestIndex;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;

public class HarvesterRDA extends HarvesterPmh {
	
	protected static final String REPO_URL = "http://researchdata.ands.org.au/registry/services/oai";
	protected final String SERVER_URL;
	
	protected static final String LABEL_GRANT = "Grant";
	protected static final String LABEL_RESEARCHER = "Researcher";
	protected static final String LABEL_INSTITUTION = "Institution";
	protected static final String LABEL_DATASET = "Dataset";
	protected static final String LABEL_PUBLICATION = "Publication";
	protected static final String LABEL_COLLECTION = "Collection";
	protected static final String LABEL_PARTY = "Party";
	protected static final String LABEL_SERVICE = "Service";
	protected static final String LABEL_ACTIVITY = "Service";

	protected static final String LABEL_RDA = "RDA";
	
	protected static final String INDEX_RDA_COLLECTION = LABEL_RDA + "_" + LABEL_COLLECTION;
	protected static final String INDEX_RDA_PARTY = LABEL_RDA + "_" + LABEL_PARTY;
	protected static final String INDEX_RDA_SERVICE = LABEL_RDA + "_" + LABEL_SERVICE;
	protected static final String INDEX_RDA_ACTIVITY = LABEL_RDA + "_" + LABEL_ACTIVITY;
	
	protected static final String FIELD_RDA_KEY	= "rda_key";
	protected static final String FIELD_NODE_TYPE = "node_type";
	protected static final String FIELD_NODE_SOURCE = "node_source";
	protected static final String FIELD_TYPE = "type";
	protected static final String FIELD_TYPE_STRING = "type_string";
	protected static final String FIELD_DATE_MODYFIED = "date_modified";
	protected static final String FIELD_ORIGINATING_SOURCE = "originating_source";
	protected static final String FIELD_ORIGINATING_SOURCE_TYPE = "originating_source_type";
	protected static final String FIELD_NAME = "name";
	protected static final String FIELD_NAME_TYPE = "name_type";
	protected static final String FIELD_POSTAL_ADDRESS = "postal_address";
	protected static final String FIELD_STREET_ADDRESS = "street_address";
	protected static final String FIELD_ADDRESS = "address";
	protected static final String FIELD_URL = "url";
	protected static final String FIELD_EMAIL = "email";
	protected static final String FIELD_SUBJECT = "subject";
	protected static final String FIELD_DESCRIPTION = "description";
	
	private static enum RelTypes implements RelationshipType
    {
    	AdminInstitute, Investigator, KnownAs
    }

    private static enum Labels implements Label {
    	RDA, Institution, Grant, Researcher, Dataset, 
    	Publication, Collection, Party, Service, Activity
    };
	
	private RestAPI graphDb;
	private RestCypherQueryEngine engine;
	
	private RestIndex<Node> indexRDACollection;
	private RestIndex<Node> indexRDAParty;
	private RestIndex<Node> indexRDAInstitution;
	private RestIndex<Node> indexRDAActivity;	
	
	public HarvesterRDA( final String serverUri ) {
		super(REPO_URL);

		// init Neo4j
		SERVER_URL = serverUri;
		graphDb = new RestAPIFacade( SERVER_URL ); 
		engine=new RestCypherQueryEngine(graphDb);  

		// Create constrants for our data
		engine.query("CREATE CONSTRAINT ON (n:" + INDEX_RDA_COLLECTION + ") ASSERT n." + FIELD_RDA_KEY + " IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:" + INDEX_RDA_PARTY + ") ASSERT n." + FIELD_RDA_KEY + " IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:" + INDEX_RDA_SERVICE + ") ASSERT n." + FIELD_RDA_KEY + " IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:" + INDEX_RDA_ACTIVITY + ") ASSERT n." + FIELD_RDA_KEY + " IS UNIQUE", Collections.<String, Object> emptyMap());

		// Obtain indexes
		indexRDACollection = graphDb.index().forNodes(INDEX_RDA_COLLECTION);
		indexRDAParty = graphDb.index().forNodes(INDEX_RDA_PARTY);
		indexRDAInstitution = graphDb.index().forNodes(INDEX_RDA_SERVICE);
		indexRDAActivity = graphDb.index().forNodes(INDEX_RDA_ACTIVITY);
		
		
	}
	
	public void Harvest() throws Exception {
		List<Record> records = GetRecords();
		
		Map<String, RestNode> mapNodes = new HashMap<String, RestNode>();

		// First create objects, next create relationships
		for (Record record : records) {
			for (RegistryObject object : record.objects) {
				
				if (mapNodes.containsKey(object.key)) {
					throw new Exception("Criticall error, the key are not unique!");
				}					
				
				if (object instanceof RegistryActivity) 
					mapNodes.put(object.key, CreateActivity((RegistryActivity) object));
				else if (object instanceof RegistryCollection) 
					mapNodes.put(object.key, CreateCollection((RegistryCollection) object)); 
				else if (object instanceof RegistryParty) 
					mapNodes.put(object.key, CreateParty((RegistryParty) object)); 
				else if (object instanceof RegistryService) 
					mapNodes.put(object.key, CreateService((RegistryService) object)); 
			}
		}
 	}
	
	protected void CopyRegistryObjectData(RegistryObject registryObject, Map<String, Object> map) {
		map.put(FIELD_RDA_KEY, registryObject.key);
		
		if (null != registryObject.originatingSource && registryObject.originatingSource.IsValid()) {
			map.put(FIELD_ORIGINATING_SOURCE_TYPE, registryObject.originatingSource.typeString);
			map.put(FIELD_ORIGINATING_SOURCE, registryObject.originatingSource.value);			
		}
		
		Phisical postalAddress = registryObject.GetPhisicalAddress(Phisical.Type.postalAddress);
		if (null != postalAddress) 
			map.put(FIELD_POSTAL_ADDRESS, postalAddress.GetAddress());
			
		Phisical streetAddress = registryObject.GetPhisicalAddress(Phisical.Type.streetAddress);
		if (null != streetAddress) 
			map.put(FIELD_STREET_ADDRESS, streetAddress.GetAddress());
		
		Phisical address = registryObject.GetPhisicalAddress(Phisical.Type.unknown);
		if (null != address) 
			map.put(FIELD_ADDRESS, address.GetAddress());
		
		Electronic email = registryObject.GetElectronicAddress(Electronic.Type.email);
		if (null != email)
			map.put(FIELD_EMAIL, email.GetAddress());
		
		Electronic url = registryObject.GetElectronicAddress(Electronic.Type.url);
		if (null != url)
			map.put(FIELD_URL, url.GetAddress());
				
		if (null != registryObject.subjects) {
			if (registryObject.subjects.size() == 1)
				map.put(FIELD_SUBJECT, registryObject.subjects.get(0).toString());
			else if (registryObject.subjects.size() > 0)
			{
				List<String> subjects = new ArrayList<String>();
				for (Subject subject : registryObject.subjects) {
					subjects.add(subject.toString());
				}
				
				map.put(FIELD_SUBJECT, subjects);
			}
		}
		
		if (null != registryObject.descriptions) {
			if (registryObject.descriptions.size() == 1)
				map.put(FIELD_DESCRIPTION, registryObject.descriptions.get(0).toString());
			else if (registryObject.descriptions.size() > 0)
			{
				List<String> descriptions = new ArrayList<String>();
				for (Description description : registryObject.descriptions) {
					descriptions.add(description.toString());
				}
				
				map.put(FIELD_DESCRIPTION, descriptions);
			}
		}
		
		// create (object)-[IdenifiesBy]->(Identifier)
		//public List<Identifier> identifiers;
		
		
		
		// create another names as (Object)-[KnownAs]->(Name)
	}
	
	protected RestNode CreateActivity(RegistryActivity registryActivity) {
		
		Map<String, Object> map = new HashMap<String, Object>();
		
		map.put(FIELD_NODE_TYPE, Labels.Activity.name());
		map.put(FIELD_NODE_SOURCE, Labels.RDA.name());
		map.put(FIELD_TYPE, registryActivity.type.name());
		map.put(FIELD_TYPE_STRING, registryActivity.typeString);
		map.put(FIELD_DATE_MODYFIED, registryActivity.dateModified);
		
		Name namePrimary = registryActivity.getPrimaryName();
		if (null == namePrimary)
			namePrimary = registryActivity.getAnyName();
		if (null != namePrimary) {		
			map.put(FIELD_NAME_TYPE, namePrimary.typeString);
			map.put(FIELD_NAME, namePrimary.GetActivityName());	
		}
		
		CopyRegistryObjectData(registryActivity, map);
							
		RestNode node = graphDb.getOrCreateNode(
				indexRDAActivity, FIELD_RDA_KEY, registryActivity.key, map);
		if (!node.hasLabel(Labels.Activity))
			node.addLabel(Labels.Activity); 
		if (!node.hasLabel(Labels.RDA))
			node.addLabel(Labels.RDA); 
		
		for (Name name : registryActivity.names) 
			if (name != namePrimary) {
			// create knownas names
			}
		
		return node;
	}
	
	protected RestNode CreateCollection(RegistryCollection registryCollection) {
		
		Map<String, Object> map = new HashMap<String, Object>();
		
		map.put(FIELD_NODE_TYPE, Labels.Collection.name());
		map.put(FIELD_NODE_SOURCE, Labels.RDA.name());
		map.put(FIELD_TYPE, registryCollection.type.name());
		map.put(FIELD_TYPE_STRING, registryCollection.typeString);
		map.put(FIELD_DATE_MODYFIED, registryCollection.dateModified);
		
		Name namePrimary = registryCollection.getPrimaryName();
		if (null == namePrimary)
			namePrimary = registryCollection.getAnyName();
		if (null != namePrimary) {		
			map.put(FIELD_NAME_TYPE, namePrimary.typeString);
			map.put(FIELD_NAME, namePrimary.GetCollectionName());	
		}
		
		CopyRegistryObjectData(registryCollection, map);
							
		RestNode node = graphDb.getOrCreateNode(
				indexRDAActivity, FIELD_RDA_KEY, registryCollection.key, map);
		if (!node.hasLabel(Labels.Collection))
			node.addLabel(Labels.Collection); 
		if (!node.hasLabel(Labels.RDA))
			node.addLabel(Labels.RDA); 
		
		for (Name name : registryCollection.names) 
			if (name != namePrimary) {
			// create knownas names
			}
		
		return node;
	}
	
	protected RestNode CreateParty(RegistryParty registryParty) {
		
		Map<String, Object> map = new HashMap<String, Object>();
		
		map.put(FIELD_NODE_TYPE, Labels.Party.name());
		map.put(FIELD_NODE_SOURCE, Labels.RDA.name());
		map.put(FIELD_TYPE, registryParty.type.name());
		map.put(FIELD_TYPE_STRING, registryParty.typeString);
		map.put(FIELD_DATE_MODYFIED, registryParty.dateModified);
		
		Name namePrimary = registryParty.getPrimaryName();
		if (null == namePrimary)
			namePrimary = registryParty.getAnyName();
		if (null != namePrimary) {		
			map.put(FIELD_NAME_TYPE, namePrimary.typeString);
			map.put(FIELD_NAME, namePrimary.GetPartyName());	
		}
		
		CopyRegistryObjectData(registryParty, map);
							
		RestNode node = graphDb.getOrCreateNode(
				indexRDAActivity, FIELD_RDA_KEY, registryParty.key, map);
		if (!node.hasLabel(Labels.Party))
			node.addLabel(Labels.Party); 
		if (!node.hasLabel(Labels.RDA))
			node.addLabel(Labels.RDA); 
		
		for (Name name : registryParty.names) 
			if (name != namePrimary) {
			// create knownas names
			}
		
		return node;
	}
	

	protected RestNode CreateService(RegistryService registryService) {
		
		Map<String, Object> map = new HashMap<String, Object>();
		
		map.put(FIELD_NODE_TYPE, Labels.Service.name());
		map.put(FIELD_NODE_SOURCE, Labels.RDA.name());
		map.put(FIELD_TYPE, registryService.type.name());
		map.put(FIELD_TYPE_STRING, registryService.typeString);
		map.put(FIELD_DATE_MODYFIED, registryService.dateModified);
		
		Name namePrimary = registryService.getPrimaryName();
		if (null == namePrimary)
			namePrimary = registryService.getAnyName();
		if (null != namePrimary) {		
			map.put(FIELD_NAME_TYPE, namePrimary.typeString);
			map.put(FIELD_NAME, namePrimary.GetServiceName());	
		}
		
		CopyRegistryObjectData(registryService, map);
							
		RestNode node = graphDb.getOrCreateNode(
				indexRDAActivity, FIELD_RDA_KEY, registryService.key, map);
		if (!node.hasLabel(Labels.Service))
			node.addLabel(Labels.Service); 
		if (!node.hasLabel(Labels.RDA))
			node.addLabel(Labels.RDA); 
		
		for (Name name : registryService.names) 
			if (name != namePrimary) {
			// create knownas names
			}
		
		return node;
	}
}
