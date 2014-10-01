package org.grants.harvesters.pmh.rda;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.grants.harvesters.pmh.registry.Relation;
import org.grants.harvesters.pmh.registry.Subject;
import org.grants.harvesters.pmh.registry.RegistryActivity.Type;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.rest.graphdb.RequestResult;
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
	protected static final String LABEL_NAME = "Name";
	protected static final String LABEL_IDENTIFICATOR = "Identificator";
	protected static final String LABEL_SUBJECT = "Subject";
	protected static final String LABEL_RDA = "RDA";
	
/*	protected static final String INDEX_RDA_COLLECTION = LABEL_RDA + "_" + LABEL_COLLECTION;
	protected static final String INDEX_RDA_PARTY = LABEL_RDA + "_" + LABEL_PARTY;
	protected static final String INDEX_RDA_SERVICE = LABEL_RDA + "_" + LABEL_SERVICE;
	protected static final String INDEX_RDA_ACTIVITY = LABEL_RDA + "_" + LABEL_ACTIVITY;
	*/
	
	protected static final String FIELD_RDA_ID	= "rda_id";
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
	protected static final String FIELD_LINK = "link";
	protected static final String FIELD_SUBJECT = "subject";
	protected static final String FIELD_SUBJECT_TYPE = "subject_type";	
	protected static final String FIELD_DESCRIPTION = "description";
	protected static final String FIELD_IDENTIFIER = "identifier";
	protected static final String FIELD_DATE_FROM = "date_from";
	protected static final String FIELD_DATE_TO = "date_to";
	protected static final String FIELD_EMAIL = "email";
	protected static final String FIELD_URL = "url";
	protected static final String FIELD_WSDL = "wsdl";
	
	protected static final String TYPE_UNKNOWN = "unknown";
	
	
	private static enum RelTypes implements RelationshipType
    {
    	AdminInstitute, Investigator, KnownAs, Name, IdentifiedBy, Subject
    }

    private static enum Labels implements Label {
    	RDA, Institution, Grant, Researcher, Dataset, 
    	Publication, Collection, Party, Service, Activity,
    	Identifier, Name, Subject
    };
	
	private RestAPI graphDb;
	private RestCypherQueryEngine engine;
	
	private RestIndex<Node> indexCollection;
	private RestIndex<Node> indexParty;
	private RestIndex<Node> indexService;
	private RestIndex<Node> indexActivity;	
	private RestIndex<Node> indexIdentificator;	
	private RestIndex<Node> indexName;		
	private RestIndex<Node> indexSubject;	
	
//	Map<String, RestNode> mapNodes = new HashMap<String, RestNode>();
	Map<String, RestNode> mapIdenitifcators = new HashMap<String, RestNode>();
	
	public HarvesterRDA( final String serverUri ) {
		super(REPO_URL);

		// init Neo4j
		SERVER_URL = serverUri;
		graphDb = new RestAPIFacade( SERVER_URL ); 
		engine=new RestCypherQueryEngine(graphDb);  

		// Create constrants for our data
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_COLLECTION + ") ASSERT n." + FIELD_RDA_ID + " IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_PARTY + ") ASSERT n." + FIELD_RDA_ID + " IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_SERVICE + ") ASSERT n." + FIELD_RDA_ID + " IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_ACTIVITY + ") ASSERT n." + FIELD_RDA_ID + " IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_IDENTIFICATOR + ") ASSERT n." + FIELD_RDA_ID + " IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_NAME + ") ASSERT n." + FIELD_RDA_ID + " IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_SUBJECT + ") ASSERT n." + FIELD_RDA_ID + " IS UNIQUE", Collections.<String, Object> emptyMap());
		
		// Obtain indexes
		indexCollection = graphDb.index().forNodes(LABEL_COLLECTION);
		indexParty = graphDb.index().forNodes(LABEL_PARTY);
		indexService = graphDb.index().forNodes(LABEL_SERVICE);
		indexActivity = graphDb.index().forNodes(LABEL_ACTIVITY);
		indexIdentificator = graphDb.index().forNodes(LABEL_IDENTIFICATOR);
		indexName = graphDb.index().forNodes(LABEL_NAME);
		indexSubject = graphDb.index().forNodes(LABEL_SUBJECT);
	}
	
	public void Harvest() throws Exception {
		System.out.println("Identifying...");
	
		if (!Identify())
			throw new Exception("Unable to Identify the service");
	
		System.out.println("Downloading sets list...");
			
		Map<String, String> mapSets = ListSets();
		if (null == mapSets )
			throw new Exception("The sets collection is empty");
		
		Set<String> keys = new HashSet<String>();
	
		// try to load whole database into memory
		for (Map.Entry<String, String> entry : mapSets.entrySet()) {
		    String set = entry.getKey();
		    String setName = entry.getValue();
		    
		    System.out.println("Processing set: " +  URLDecoder.decode(setName, "UTF-8"));
		    
		    List<Record> records = ListRecords(set, MetadataPrefix.rif, null, null);
		    
		    if (null != records) {
		    	System.out.println("Retrieved " + records.size() + " records");
		    
			    for (Record record : records) 
					for (RegistryObject object : record.objects) {
						
						if (keys.contains(object.key)) {
							System.out.println("The key: \'" + object.key + "' already exists in the database, the record will be ignored");
							continue;
						} else
							keys.add(object.key);
						
						if (object instanceof RegistryActivity)
							CreateActivity((RegistryActivity) object);
						else if (object instanceof RegistryCollection)
							CreateCollection((RegistryCollection) object); 
						else if (object instanceof RegistryParty)
							CreateParty((RegistryParty) object);
						else if (object instanceof RegistryService)
							CreateService((RegistryService) object);
					}
		    }
		}
	
		/*
		// try to load whole database into memory
		for (Map.Entry<String, String> entry : mapSets.entrySet()) {
		    String set = entry.getKey();
		    String setName = entry.getValue();
		    
		    System.out.println("Processing set: " +  URLDecoder.decode(setName, "UTF-8"));
		    
		    List<Record> records = ListRecords(set, MetadataPrefix.rif, null, null);
		    
		    if (null != records) {
		    	System.out.println("Retrieved " + records.size() + " records");
	
			    // now create relationships
				for (Record record : records) 
					for (RegistryObject object : record.objects) 
						if (null != object.relatedObjects) {
							RestNode nodeObject = mapNodes.get(object.key);
							if (null != nodeObject)
								for (RelatedObject relatedObject : object.relatedObjects) {
									RestNode nodeRelatedObject = mapNodes.get(relatedObject.key); 
									if (null != nodeRelatedObject) 
										for (Relation relation : relatedObject.relations) {
											/*	Map<String, Object> props = new HashMap<String, Object>();
												if (null != relation.url)
													props.put("url", relation.url);
												if (null != relation.description)
													props.put("description", relation.description);
														
												Map<String, Object> data = MapUtil.map(
														"to", nodeRelatedObject.getUri(), 
														"type", relation.typeString);
												if (props != null && props.size() > 0) 
													data.put("data", props);
												
												RequestResult requestResult = graphDb.current().getRestRequest().with(
														nodeObject.getUri()).post("relationships", data);
									        graphDb.createRestRelationship(requestResult, nodeObject);* /
		
											Map<String, Object> props = new HashMap<String, Object>();
											if (null != relation.url)
												props.put("url", relation.url);
											if (null != relation.description)
												props.put("description", relation.description);
											
											CreateUniqueRelationship(nodeObject, nodeRelatedObject, 
													DynamicRelationshipType.withName(relation.typeString));
										}						
								}
						}
		    }
		}*/
 	}
	
	protected void addMapElement(Map<String, Object> map, String fieldName, String typeName,  
			List<String> list) {
		if (null != typeName && typeName.length() > 0 && !typeName.equals(TYPE_UNKNOWN)) {
			if (null == fieldName)
				fieldName = typeName;
			else 
				fieldName += "_" + typeName;
		}
		
		addMapElement(map, fieldName, typeName,  list);
	}
	
	protected void addMapElement(Map<String, Object> map, String fieldName, List<String> list) {
		if (null != list) {
			if (null != fieldName && fieldName.length() > 0) {
				if (list.size() == 1)
					map.put(fieldName, list.get(0));
				else 
					map.put(fieldName, list);
			}
		}
	}
	
	protected void CopyRegistryObjectData(RegistryObject registryObject, Map<String, Object> map) {
		map.put(FIELD_RDA_ID, registryObject.key);
		
		if (null != registryObject.originatingSource && registryObject.originatingSource.IsValid()) {
			if (null != registryObject.originatingSource.typeString && !registryObject.originatingSource.typeString.isEmpty())
				map.put(FIELD_ORIGINATING_SOURCE_TYPE, registryObject.originatingSource.typeString);
			map.put(FIELD_ORIGINATING_SOURCE, registryObject.originatingSource.value);			
		}
		
		// save primary name if it exists
		addMapElement(map, FIELD_NAME, registryObject.getNames(Name.Type.primary));
		
		// save ANY existing address
		List<String> address = registryObject.getPhisicalAddresses(Phisical.Type.postal);
		if (null == address)
			address = registryObject.getPhisicalAddresses(Phisical.Type.street);
		if (null == address)
			address = registryObject.getPhisicalAddresses(Phisical.Type.unknown);
		if (null != address)
			addMapElement(map, FIELD_ADDRESS, address);		 
		
		addMapElement(map, FIELD_EMAIL, registryObject.getElectronicAddresses(Electronic.Type.email));
		addMapElement(map, FIELD_URL, registryObject.getElectronicAddresses(Electronic.Type.url));
		addMapElement(map, FIELD_WSDL, registryObject.getElectronicAddresses(Electronic.Type.wsdl));
		addMapElement(map, FIELD_SUBJECT, registryObject.getSubjects());
		addMapElement(map, FIELD_DESCRIPTION, registryObject.getDescriptions());
		
		// Save address if it extst
		/*
		for (Phisical.Type phisicalType : Phisical.Type.values()) 
			addMapElement(map, FIELD_ADDRESS, phisicalType.name(),  
					 registryObject.getPhisicalAddresses(phisicalType));
	
		for (Electronic.Type electronicType : Electronic.Type.values()) 
			addMapElement(map, FIELD_LINK, electronicType.name(),  
					 registryObject.getElectronicAddresses(electronicType));
				 
		for (Identifier.Type identifierType : Identifier.Type.values()) 
			addMapElement(map, FIELD_IDENTIFIER, identifierType.name(),  
					 registryObject.getIdentifiers(identifierType));
		
		for (Name.Type nameType : Name.Type.values()) 
			addMapElement(map, FIELD_NAME, nameType.name(),  
					 registryObject.getNames(nameType));*/
	}
	
	protected RestNode CreateActivity(RegistryActivity registryActivity) {
		
		Map<String, Object> map = new HashMap<String, Object>();
		
		map.put(FIELD_NODE_TYPE, Labels.Activity.name());
		map.put(FIELD_NODE_SOURCE, Labels.RDA.name());
		map.put(FIELD_TYPE, registryActivity.type.name());
		if (null != registryActivity.typeString && !registryActivity.typeString.isEmpty())
			map.put(FIELD_TYPE_STRING, registryActivity.typeString);
		if (null != registryActivity.dateModified && !registryActivity.dateModified.isEmpty())
			map.put(FIELD_DATE_MODYFIED, registryActivity.dateModified);
		
		
		CopyRegistryObjectData(registryActivity, map);
							
		RestNode node = CreateUniqueNode(indexActivity, FIELD_RDA_ID, registryActivity.key, 
				Labels.Activity, map);
		createRegistryObjectsNodes(registryActivity, node);
		
		return node;
	}
	
	protected RestNode CreateCollection(RegistryCollection registryCollection) {
		
		Map<String, Object> map = new HashMap<String, Object>();
		
		map.put(FIELD_NODE_TYPE, Labels.Collection.name());
		map.put(FIELD_NODE_SOURCE, Labels.RDA.name());
		map.put(FIELD_TYPE, registryCollection.type.name());
		if (null != registryCollection.typeString && !registryCollection.typeString.isEmpty())
			map.put(FIELD_TYPE_STRING, registryCollection.typeString);
		if (null != registryCollection.dateModified && !registryCollection.dateModified.isEmpty())
			map.put(FIELD_DATE_MODYFIED, registryCollection.dateModified);
		
		CopyRegistryObjectData(registryCollection, map);
							
		RestNode node = CreateUniqueNode(indexCollection, FIELD_RDA_ID, registryCollection.key, 
				Labels.Collection, map);
		
		createRegistryObjectsNodes(registryCollection, node);
			
		return node;
	}
	
	protected RestNode CreateParty(RegistryParty registryParty) {
		
		Map<String, Object> map = new HashMap<String, Object>();
		
		map.put(FIELD_NODE_TYPE, Labels.Party.name());
		map.put(FIELD_NODE_SOURCE, Labels.RDA.name());
		map.put(FIELD_TYPE, registryParty.type.name());
		if (null != registryParty.typeString && !registryParty.typeString.isEmpty())
			map.put(FIELD_TYPE_STRING, registryParty.typeString);
		if (null != registryParty.dateModified && !registryParty.dateModified.isEmpty())
			map.put(FIELD_DATE_MODYFIED, registryParty.dateModified);
	
		CopyRegistryObjectData(registryParty, map);
	
		RestNode node = CreateUniqueNode(indexParty, FIELD_RDA_ID, registryParty.key, 
				Labels.Party, map);
		createRegistryObjectsNodes(registryParty, node);
		
		return node;
	}
	

	protected RestNode CreateService(RegistryService registryService) {
		
		Map<String, Object> map = new HashMap<String, Object>();
		
		map.put(FIELD_NODE_TYPE, Labels.Service.name());
		map.put(FIELD_NODE_SOURCE, Labels.RDA.name());
		map.put(FIELD_TYPE, registryService.type.name());
		if (null != registryService.typeString && !registryService.typeString.isEmpty())
			map.put(FIELD_TYPE_STRING, registryService.typeString);
		if (null != registryService.dateModified && !registryService.dateModified.isEmpty())
			map.put(FIELD_DATE_MODYFIED, registryService.dateModified);
	
		CopyRegistryObjectData(registryService, map);
							
		RestNode node = CreateUniqueNode(indexService, FIELD_RDA_ID, registryService.key, 
				Labels.Service, map);
		createRegistryObjectsNodes(registryService, node);
		
		return node;
	}
	
	protected void createRegistryObjectsNodes(RegistryObject registryObject, RestNode nodeObject) {
		// Create Name objects		
		if (null != registryObject.names) 
			for (Name name : registryObject.names) 
				createName(name, registryObject.key, nodeObject);
		
		// Create Identefied objects
		if (null != registryObject.identifiers) 
			for (Identifier identifier : registryObject.identifiers) 
				createIdentifier(identifier, nodeObject);
		
		// Create Subject objects
		if (null != registryObject.subjects)
			for (Subject subject : registryObject.subjects)
				createSubject(subject, registryObject.key, nodeObject);
	}

	protected void createName(Name name, String key, RestNode nodeObject) {
		if (name.isValid()) {
			String nameString = name.toString();
			String keyString = key + "/" + name.type.name() + "/" + nameString;
			
			Map<String, Object> map = new HashMap<String, Object>();
			
			map.put(FIELD_RDA_ID, keyString);
			map.put(FIELD_NODE_TYPE, Labels.Name.name());
			map.put(FIELD_NODE_SOURCE, Labels.RDA.name());
			map.put(FIELD_TYPE, name.type.name());
			map.put(FIELD_TYPE_STRING, name.typeString);
			map.put(FIELD_NAME, nameString);
			if (null != name.dateFrom)
				map.put(FIELD_DATE_FROM, name.dateFrom);
			if (null != name.dateTo)
				map.put(FIELD_DATE_TO, name.dateTo);
			
			RestNode node = CreateUniqueNode(indexName, FIELD_RDA_ID, keyString, 
					Labels.Name, map);
			CreateUniqueRelationship(nodeObject, node, RelTypes.Name);
		}
	}
	
	protected void createIdentifier(Identifier identifier, RestNode nodeObject) {
		if (identifier.isValid()) {
			String idString = identifier.toString();
			String keyString = identifier.type.name() + "/" + idString;
			
			Map<String, Object> map = new HashMap<String, Object>();
			
			map.put(FIELD_RDA_ID, keyString);
			map.put(FIELD_NODE_TYPE, Labels.Identifier.name());
			map.put(FIELD_NODE_SOURCE, Labels.RDA.name());
			map.put(FIELD_TYPE, identifier.type.name());
			map.put(FIELD_TYPE_STRING, identifier.typeString);
			map.put(FIELD_IDENTIFIER, idString);

			RestNode node = CreateUniqueNode(indexIdentificator, FIELD_RDA_ID, keyString, 
					Labels.Identifier, map);
			CreateUniqueRelationship(nodeObject, node, RelTypes.IdentifiedBy);
		}
	}
	
	protected void createSubject(Subject subject, String key, RestNode nodeObject) {
		if (subject.isValid()) {
			String subjectString = subject.toString();
			String keyString = key + "/" + subject.typeString + "/" + subjectString;
			
			Map<String, Object> map = new HashMap<String, Object>();
			
			map.put(FIELD_RDA_ID, keyString);
			map.put(FIELD_NODE_TYPE, Labels.Subject.name());
			map.put(FIELD_NODE_SOURCE, Labels.RDA.name());
			map.put(FIELD_TYPE_STRING, subject.typeString);
			map.put(FIELD_IDENTIFIER, subjectString);
			
			RestNode node = CreateUniqueNode(indexSubject, FIELD_RDA_ID, keyString, 
					Labels.Subject, map);
			CreateUniqueRelationship(nodeObject, node, RelTypes.Subject);
		}
	}
	
	/*
	protected void createRelatedInfo(RelatedInfo relatedInfo, RestNode nodeObject) {
		if (relatedInfo.isValid()) {
			String idString = identifier.toString();
			String keyString = identifier.type.name() + "/" + idString;
			
			Map<String, Object> map = new HashMap<String, Object>();
			
			map.put(FIELD_RDA_ID, keyString);
			map.put(FIELD_NODE_TYPE, Labels.Identifier.name());
			map.put(FIELD_NODE_SOURCE, Labels.RDA.name());
			map.put(FIELD_TYPE, identifier.type.name());
			map.put(FIELD_TYPE_STRING, identifier.typeString);
			map.put(FIELD_IDENTIFIER, idString);
			
			RestNode node = graphDb.getOrCreateNode(
					indexIdentificator, FIELD_RDA_ID, keyString, map);
			if (!node.hasLabel(Labels.Identifier))
				node.addLabel(Labels.Identifier); 
			if (!node.hasLabel(Labels.RDA))
				node.addLabel(Labels.RDA); 
			
			nodeRegistryObject.createRelationshipTo(node, RelTypes.IdentifiedBy);		
		}
	}*/
	
	private RestNode CreateUniqueNode(RestIndex<Node> index, final String key, final Object value,
			Label label, Map<String, Object> pars) {
		RestNode node = graphDb.getOrCreateNode(
				index, key, value, pars);
		if (!node.hasLabel(label))
			node.addLabel(label); 
		if (!node.hasLabel(Labels.RDA))
			node.addLabel(Labels.RDA);
		
		return node;
	}
	
	private void CreateUniqueRelationship(RestNode nodeStart, RestNode nodeEnd, 
			RelationshipType type) {
		CreateUniqueRelationship(nodeStart, nodeEnd, type, Direction.OUTGOING);
	}
	
	private void CreateUniqueRelationship(RestNode nodeStart, RestNode nodeEnd, 
			RelationshipType type, Direction direction) {
		// get all node relationships. They should be empty for a new node
		Iterable<Relationship> rels = nodeStart.getRelationships(type, direction);
		
		for (Relationship rel : rels) {
			if (direction == Direction.INCOMING) {
				if (rel.getStartNode().getId() == nodeEnd.getId())
					return;
			} else if (direction == Direction.OUTGOING) {
				if (rel.getEndNode().getId() == nodeEnd.getId())
					return;
			} else {
				if (rel.getStartNode().getId() == nodeEnd.getId() ||
					rel.getEndNode().getId() == nodeEnd.getId())
					return;
			} 
		}
		
		if (direction == Direction.INCOMING) 
			nodeEnd.createRelationshipTo(nodeStart, type);
		else
			nodeStart.createRelationshipTo(nodeEnd, type);
	}
}
