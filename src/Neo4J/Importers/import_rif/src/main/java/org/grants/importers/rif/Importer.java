package org.grants.importers.rif;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.neo4j.graphdb.Direction;
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
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.StatusType;

import au.org.ands.standards.rif_cs.registryobjects.Activity;
import au.org.ands.standards.rif_cs.registryobjects.Collection;
import au.org.ands.standards.rif_cs.registryobjects.DatesType;
import au.org.ands.standards.rif_cs.registryobjects.DescriptionType;
import au.org.ands.standards.rif_cs.registryobjects.IdentifierType;
import au.org.ands.standards.rif_cs.registryobjects.NameType;
import au.org.ands.standards.rif_cs.registryobjects.Party;
import au.org.ands.standards.rif_cs.registryobjects.RegistryObjects;
import au.org.ands.standards.rif_cs.registryobjects.RelatedObjectType;
import au.org.ands.standards.rif_cs.registryobjects.RelationType;
import au.org.ands.standards.rif_cs.registryobjects.Service;
import au.org.ands.standards.rif_cs.registryobjects.SubjectType;

public class Importer {

	private static final String LABEL_COLLECTION = "Collection";
	private static final String LABEL_ACTIVITY = "Activity";
	private static final String LABEL_SERVICE = "Service";
	private static final String LABEL_PARTY = "Party";
	
	private static final String LABEL_RDA = "Dryad";
	private static final String LABEL_RDA_COLLECTION = LABEL_RDA + "_" + LABEL_COLLECTION;
	private static final String LABEL_RDA_ACTIVITY = LABEL_RDA + "_" + LABEL_ACTIVITY;
	private static final String LABEL_RDA_SERVICE = LABEL_RDA + "_" + LABEL_SERVICE;
	private static final String LABEL_RDA_PARTY = LABEL_RDA + "_" + LABEL_PARTY;
	
	private static final String RELATION_RELATED_TO = "relatedTo";
	
	private static final String PROPERTY_KEY = "key";
	private static final String PROPERTY_RDA_KEY = "rda_key";
	private static final String PROPERTY_NODE_SOURCE = "node_source";
	private static final String PROPERTY_NODE_TYPE = "node_type";
	private static final String PROPERTY_TYPE = "type";
	private static final String PROPERTY_DATE_MODIFIED = "date_modified";
	private static final String PROPERTY_DATE_ACCESSIONED = "date_accessioned";
	private static final String PROPERTY_IDENTIFIER = "identifier";
	private static final String PROPERTY_NAME = "name";
	private static final String PROPERTY_DATE = "date";
	private static final String PROPERTY_SUBJECT = "subject";
	private static final String PROPERTY_DESCRIPTION = "subject";
	private static final String PROPERTY_URL = "url";
	
	private static final String NAME_PART_FAMILY = "family";
	private static final String NAME_PART_GIVEN = "given";
	private static final String NAME_PART_SUFFIX = "suffix";
	private static final String NAME_PART_TITLE = "title";
	
	private static final String DATE_FROM = "dateFrom";
	private static final String DATE_TO = "dateTo";
	
    private static enum Labels implements Label {
    	RDA, Collection, Service, Party, Activity
    };
	
	private final String folderUri;
	
	private RestAPI graphDb;
	private RestCypherQueryEngine engine;
	
	private RestIndex<Node> indexRDACollection;
	private RestIndex<Node> indexRDAService;
	private RestIndex<Node> indexRDAParty;
	private RestIndex<Node> indexRDAActivity;
	
	
	private List<RelatedObject> relatedObjects = new ArrayList<RelatedObject>();

	private Unmarshaller unmarshaller;
	
	private int deletedRecords = 0;
	private int brokenRecords = 0;
	private int createdRecords = 0;
	private int createdRelationships = 0;
	
	public Importer( final String folderUri, final String neo4jUrl ) throws JAXBException {
		
		System.out.println("Connecting to Neo4J server at: " + neo4jUrl);
		
		graphDb = new RestAPIFacade(neo4jUrl); //"http://localhost:7474/db/data/");  
		engine = new RestCypherQueryEngine(graphDb);  
		
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_RDA_COLLECTION + ") ASSERT n." + PROPERTY_KEY + " IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_RDA_ACTIVITY + ") ASSERT n." + PROPERTY_KEY + " IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_RDA_SERVICE + ") ASSERT n." + PROPERTY_KEY + " IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_RDA_PARTY + ") ASSERT n." + PROPERTY_KEY + " IS UNIQUE", Collections.<String, Object> emptyMap());

		engine.query("CREATE INDEX ON :RDA(key)", Collections.<String, Object> emptyMap());
		engine.query("CREATE INDEX ON :RDA(rda_key)", Collections.<String, Object> emptyMap());
		
		indexRDACollection = graphDb.index().forNodes(LABEL_RDA_COLLECTION);
		indexRDAActivity = graphDb.index().forNodes(LABEL_RDA_ACTIVITY);
		indexRDAService = graphDb.index().forNodes(LABEL_RDA_SERVICE);
		indexRDAParty = graphDb.index().forNodes(LABEL_RDA_PARTY);
		
		unmarshaller = JAXBContext.newInstance( "org.openarchives.oai._2:au.org.ands.standards.rif_cs.registryobjects:au.org.ands.standards.rif_cs.extendedregistryobjects" ).createUnmarshaller();
		
		this.folderUri = folderUri;		
	}		
	
	public void importRecords() {
		deletedRecords = 0;
		brokenRecords = 0;
		createdRecords = 0;
		createdRelationships = 0;
		
		File[] folders = new File(folderUri).listFiles();
		for (File folder : folders) 
			if (folder.isDirectory() && !folder.getName().equals("_cache")) {
				File[] files = folder.listFiles();
				for (File file : files)  
					if (!file.isDirectory()) 				
						importRecord(file);
			}
		
		for (RelatedObject relatedObject : relatedObjects) {
			 RestNode nodeFrom = findRDADatasetByKey(relatedObject.key);
			 RestNode nodeTo = findRDADatasetByKey(relatedObject.relatedKey);
			 
			 if (null != nodeFrom && null != nodeTo) {				 
				 String relation = relatedObject.relationshipName;
				 if (null == relation || relation.isEmpty()) 
					 relation = RELATION_RELATED_TO; 
				 
				 System.out.println("Relationship: (" + relatedObject.key + ")-[" + relation + "]->(" + relatedObject.relatedKey + ")");
				  
				 Map<String, Object> pars = null;
				 if (relatedObject.description != null && !relatedObject.description.isEmpty() 
						 || relatedObject.url != null && !relatedObject.url.isEmpty()) {
					 
					 pars = new HashMap<String, Object>();
					 if (relatedObject.description != null) {
						 if (relatedObject.description.size() == 1)
							 pars.put(PROPERTY_DESCRIPTION, relatedObject.description.get(0));
						 else if (relatedObject.description.size() > 1)
							 pars.put(PROPERTY_DESCRIPTION, relatedObject.description);
					 }
					 
					 if (relatedObject.url != null) {
						 if (relatedObject.url.size() == 1)
							 pars.put(PROPERTY_URL, relatedObject.url.get(0));
						 else if (relatedObject.url.size() > 1)
							 pars.put(PROPERTY_URL, relatedObject.url);
					 }
				 }
				 
				 createUniqueRelationship(nodeFrom, nodeTo, 
						 DynamicRelationshipType.withName(relation), Direction.OUTGOING, pars);	
			 }
		}
				
		System.out.println("Done. Created " + createdRecords + " records and " + createdRelationships + " relationships. Detected " + deletedRecords + " deleted and " + brokenRecords + " broken records.");
	}
	
	/*
	private List<RestNode> findNodeByDoi(final String doi) {
		List<RestNode> result = null;
		
		Map<String, Object> pars = new HashMap<String, Object>();
		pars.put(PROPERTY_DOI, doi);
		
		QueryResult<Map<String, Object>> nodes = engine.query(CYPHER_FIND_NODE_BY_DOI, pars);
		for (Map<String, Object> row : nodes) {
			RestNode node = (RestNode) row.get(PROPERTY_N);
			if (null != node) {
				if (null == result)
					result = new ArrayList<RestNode>();
				
				result.add(node);
			}
		}
		
		return result;
	}
	*/
	/*
	private static Element findXmlElement(Element xml, String name) {
		if (xml.getLocalName().equals(name))
			return xml;

		return null;
	}
	*/
	
	/*
	private static Element findXmlElement(List<Object> xmls, String name) {
		for (Object xml : xmls) 
			if (xml instanceof Element && ((Element) xml).getLocalName().equals(name))
				return (Element) xml;

		return null;
	}
	
	private static Element findXmlElement(NodeList xmls, String name) {
		if (null != xmls) {
			int length = xmls.getLength();
			for (int i = 0; i < length; ++i) {
				org.w3c.dom.Node xml = xmls.item(i);
				if (xml instanceof Element && ((Element) xml).getLocalName().equals(name))
					return (Element) xml;
			}
		}

		return null;
	}	
	
	private static List<Element> findXmlElements(List<Object> xmls, String name) {
		List<Element> list = null;		
		for (Object xml : xmls) 
			if (xml instanceof Element && ((Element) xml).getLocalName().equals(name)) {
				if (null == list)
					list = new ArrayList<Element>();
				
				list.add((Element) xml);
			}

		return list;
	}
	
	private static List<Element> findXmlElements(NodeList xmls, String name) {
		List<Element> list = null;		
		
		int length = xmls.getLength();
		for (int i = 0; i < length; ++i) {
			org.w3c.dom.Node xml = xmls.item(i);
			if (xml instanceof Element && ((Element) xml).getLocalName().equals(name)) {
				if (null == list)
					list = new ArrayList<Element>();
				
				list.add((Element) xml);
			}
		}

		return list;
	}*/
	
	
	@SuppressWarnings("unchecked")
	private void importRecord(File fileXml) {
		try {
			JAXBElement<?> element = (JAXBElement<?>) unmarshaller.unmarshal( new FileInputStream( fileXml ) );
			
			RecordType record = (RecordType) element.getValue();	
			HeaderType header = record.getHeader();
			
			StatusType status = header.getStatus();
			if (status == StatusType.DELETED) {
				++deletedRecords;
				return;
			}
			
			String idetifier = header.getIdentifier();
			System.out.println("Record: " + idetifier.toString());
		//	String datestamp = header.getDatestamp();
//			System.out.println(datestamp.toString());
//			List<String> specs = header.getSetSpec();
			
			/*
			Map<String, Object> map = new HashMap<String, Object>();
			map.put(PROPERTY_KEY, idetifier);
			map.put(PROPERTY_TIMESTAMP, datestamp);
			map.put(PROPERTY_NODE_SOURCE, LABEL_RDA);
//			map.put(PROPERTY_NODE_TYPE, LABEL_RECORD);*/
			
			if (null != record.getMetadata()) {
				Object metadata = record.getMetadata().getAny();
				if (metadata instanceof RegistryObjects) {
					RegistryObjects registryObjects = (RegistryObjects) metadata;
					if (registryObjects.getRegistryObject() != null && registryObjects.getRegistryObject().size() > 0) {
						for (RegistryObjects.RegistryObject registryObject : registryObjects.getRegistryObject()) {
							String key = registryObject.getKey();
							
							if (registryObject.getCollection() != null) 
								importCollection(idetifier, key, registryObject.getCollection());
							else if (registryObject.getActivity() != null)
								importActivity(idetifier, key, registryObject.getActivity());
							else if (registryObject.getService() != null) 
								importService(idetifier, key, registryObject.getService());
							else if (registryObject.getParty() != null) 
								importParty(idetifier, key, registryObject.getParty());
							else
								System.out.println("The record is empty!");
							
							++createdRecords;

						}
						
						// at this point all registry objects should be imported, abort the function
						return;
					} else
						System.out.println("Metadata does not contains any records");
				} else
					System.out.println("Metadata is not in rif format");
			} else
				System.out.println("Unable to find metadata");
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		
		++brokenRecords;
	}
	
	private void importCollection(final String idetifier, final String key, Collection collection) {
		Map<String, Object> map = new HashMap<String, Object>();
		
		map.put(PROPERTY_KEY, key);
		map.put(PROPERTY_RDA_KEY, idetifier);
		map.put(PROPERTY_NODE_SOURCE, LABEL_RDA);
		map.put(PROPERTY_NODE_TYPE, LABEL_COLLECTION);
		
		addData(map, PROPERTY_TYPE, collection.getType());
		addData(map, PROPERTY_DATE_MODIFIED, collection.getDateModified());
		addData(map, PROPERTY_DATE_ACCESSIONED, collection.getDateAccessioned());
		
		for (Object object : collection.getIdentifierOrNameOrDates()) {
			if (object instanceof IdentifierType) {
				IdentifierType identifier = (IdentifierType) object;
				String type = identifier.getType();
				if (null == type || type.isEmpty())
					type = PROPERTY_IDENTIFIER;
				else
					type = PROPERTY_IDENTIFIER + "_" + type;
				addData(map, type, identifier.getValue());
			} else if (object instanceof NameType) { 
				NameType name = (NameType) object;
				
				String lang = name.getLang();
				String dateFrom = name.getDateFrom();
				String dateTo = name.getDateTo();
				
				if (null == lang && null == dateFrom && null == dateTo) {
					String type = name.getType();
					if (null == type || type.isEmpty())
						type = PROPERTY_NAME;
					else
						type = PROPERTY_NAME + "_" + type;
					
					addData(map, type, getFullName(name));
				}
			} else if (object instanceof DatesType) {
				DatesType dates = (DatesType) object;
				
				String type = dates.getType();
				if (null == type || type.isEmpty())
					type = PROPERTY_DATE;
				else
					type = PROPERTY_DATE + "_" + type;
				
				addData(map, type, getFullDate(dates));				
			} /*else if (object instanceof LocationType) {
				LocationType location = (LocationType) object;
			} */ else if (object instanceof SubjectType) {
				SubjectType subject = (SubjectType) object;
				
				String type = subject.getType();
				if (null == type || type.isEmpty())
					type = PROPERTY_SUBJECT;
				else
					type = PROPERTY_SUBJECT + "_" + type;
				
				addData(map, type, subject.getValue());		
			} else if (object instanceof DescriptionType) {
				DescriptionType description = (DescriptionType) object;

				String type = description.getType();
				if (null == type || type.isEmpty())
					type = PROPERTY_SUBJECT;
				else
					type = PROPERTY_SUBJECT + "_" + type;
				
				addData(map, type, description.getValue());		
			} else if (object instanceof RelatedObjectType) {
				RelatedObjectType relatedObject = (RelatedObjectType) object;
				for (RelationType relType : relatedObject.getRelation()) {
					RelatedObject relObject = new RelatedObject();
					
					relObject.key = key;
					relObject.relatedKey = relatedObject.getKey();
					relObject.relationshipName = relType.getType();
					
					for (Object descriptionOrUrl : relType.getDescriptionOrUrl()) {
						if (descriptionOrUrl instanceof RelationType.Description) 
							relObject.addDescription(((RelationType.Description)descriptionOrUrl).getValue());
						else if (descriptionOrUrl instanceof String)
							relObject.addUrl((String) descriptionOrUrl);
					}
					
					relatedObjects.add(relObject);
				}				
			}			
		}
		
		RestNode node = graphDb.getOrCreateNode(indexRDACollection, PROPERTY_KEY, key, map);
		if (!node.hasLabel(Labels.Collection))
			node.addLabel(Labels.Collection); 
		if (!node.hasLabel(Labels.RDA))
			node.addLabel(Labels.RDA);
	}
	
	private void importActivity(final String idetifier, final String key, Activity activity) {
		Map<String, Object> map = new HashMap<String, Object>();
		
		map.put(PROPERTY_KEY, key);
		map.put(PROPERTY_RDA_KEY, idetifier);
		map.put(PROPERTY_NODE_SOURCE, LABEL_RDA);
		map.put(PROPERTY_NODE_TYPE, LABEL_ACTIVITY);
		
		addData(map, PROPERTY_TYPE, activity.getType());
		addData(map, PROPERTY_DATE_MODIFIED, activity.getDateModified());
		
		for (Object object : activity.getIdentifierOrNameOrLocation()) {
			if (object instanceof IdentifierType) {
				IdentifierType identifier = (IdentifierType) object;
				String type = identifier.getType();
				if (null == type || type.isEmpty())
					type = PROPERTY_IDENTIFIER;
				else
					type = PROPERTY_IDENTIFIER + "_" + type;
				addData(map, type, identifier.getValue());
			} else if (object instanceof NameType) { 
				NameType name = (NameType) object;
				
				String lang = name.getLang();
				String dateFrom = name.getDateFrom();
				String dateTo = name.getDateTo();
				
				if (null == lang && null == dateFrom && null == dateTo) {
					String type = name.getType();
					if (null == type || type.isEmpty())
						type = PROPERTY_NAME;
					else
						type = PROPERTY_NAME + "_" + type;
					
					addData(map, type, getFullName(name));
				}
			} /*else if (object instanceof LocationType) {
				LocationType location = (LocationType) object;
			} */ else if (object instanceof SubjectType) {
				SubjectType subject = (SubjectType) object;
				
				String type = subject.getType();
				if (null == type || type.isEmpty())
					type = PROPERTY_SUBJECT;
				else
					type = PROPERTY_SUBJECT + "_" + type;
				
				addData(map, type, subject.getValue());		
			} else if (object instanceof DescriptionType) {
				DescriptionType description = (DescriptionType) object;

				String type = description.getType();
				if (null == type || type.isEmpty())
					type = PROPERTY_SUBJECT;
				else
					type = PROPERTY_SUBJECT + "_" + type;
				
				addData(map, type, description.getValue());		
			} else if (object instanceof RelatedObjectType) {
				RelatedObjectType relatedObject = (RelatedObjectType) object;
				for (RelationType relType : relatedObject.getRelation()) {
					RelatedObject relObject = new RelatedObject();
					
					relObject.key = key;
					relObject.relatedKey = relatedObject.getKey();
					relObject.relationshipName = relType.getType();
					
					for (Object descriptionOrUrl : relType.getDescriptionOrUrl()) {
						if (descriptionOrUrl instanceof RelationType.Description) 
							relObject.addDescription(((RelationType.Description)descriptionOrUrl).getValue());
						else if (descriptionOrUrl instanceof String)
							relObject.addUrl((String) descriptionOrUrl);
					}
					
					relatedObjects.add(relObject);
				}				
			}			
		}
		
		RestNode node = graphDb.getOrCreateNode(indexRDAActivity, PROPERTY_KEY, key, map);
		if (!node.hasLabel(Labels.Activity))
			node.addLabel(Labels.Activity); 
		if (!node.hasLabel(Labels.RDA))
			node.addLabel(Labels.RDA);
	}
	
	private void importService(final String idetifier, final String key, Service service) {
		Map<String, Object> map = new HashMap<String, Object>();
		
		map.put(PROPERTY_KEY, key);
		map.put(PROPERTY_RDA_KEY, idetifier);
		map.put(PROPERTY_NODE_SOURCE, LABEL_RDA);
		map.put(PROPERTY_NODE_TYPE, LABEL_SERVICE);
		
		addData(map, PROPERTY_TYPE, service.getType());
		addData(map, PROPERTY_DATE_MODIFIED, service.getDateModified());
		
		for (Object object : service.getIdentifierOrNameOrLocation()) {
			if (object instanceof IdentifierType) {
				IdentifierType identifier = (IdentifierType) object;
				String type = identifier.getType();
				if (null == type || type.isEmpty())
					type = PROPERTY_IDENTIFIER;
				else
					type = PROPERTY_IDENTIFIER + "_" + type;
				addData(map, type, identifier.getValue());
			} else if (object instanceof NameType) { 
				NameType name = (NameType) object;
				
				String lang = name.getLang();
				String dateFrom = name.getDateFrom();
				String dateTo = name.getDateTo();
				
				if (null == lang && null == dateFrom && null == dateTo) {
					String type = name.getType();
					if (null == type || type.isEmpty())
						type = PROPERTY_NAME;
					else
						type = PROPERTY_NAME + "_" + type;
					
					addData(map, type, getFullName(name));
				}
			} /*else if (object instanceof LocationType) {
				LocationType location = (LocationType) object;
			} */ else if (object instanceof SubjectType) {
				SubjectType subject = (SubjectType) object;
				
				String type = subject.getType();
				if (null == type || type.isEmpty())
					type = PROPERTY_SUBJECT;
				else
					type = PROPERTY_SUBJECT + "_" + type;
				
				addData(map, type, subject.getValue());		
			} else if (object instanceof DescriptionType) {
				DescriptionType description = (DescriptionType) object;

				String type = description.getType();
				if (null == type || type.isEmpty())
					type = PROPERTY_SUBJECT;
				else
					type = PROPERTY_SUBJECT + "_" + type;
				
				addData(map, type, description.getValue());		
			} else if (object instanceof RelatedObjectType) {
				RelatedObjectType relatedObject = (RelatedObjectType) object;
				for (RelationType relType : relatedObject.getRelation()) {
					RelatedObject relObject = new RelatedObject();
					
					relObject.key = key;
					relObject.relatedKey = relatedObject.getKey();
					relObject.relationshipName = relType.getType();
					
					for (Object descriptionOrUrl : relType.getDescriptionOrUrl()) {
						if (descriptionOrUrl instanceof RelationType.Description) 
							relObject.addDescription(((RelationType.Description)descriptionOrUrl).getValue());
						else if (descriptionOrUrl instanceof String)
							relObject.addUrl((String) descriptionOrUrl);
					}
					
					relatedObjects.add(relObject);
				}				
			}			
		}
		
		RestNode node = graphDb.getOrCreateNode(indexRDAService, PROPERTY_KEY, key, map);
		if (!node.hasLabel(Labels.Service))
			node.addLabel(Labels.Service); 
		if (!node.hasLabel(Labels.RDA))
			node.addLabel(Labels.RDA);
	}
	
	private void importParty(final String idetifier, final String key, Party party) {
		Map<String, Object> map = new HashMap<String, Object>();
		
		map.put(PROPERTY_KEY, key);
		map.put(PROPERTY_RDA_KEY, idetifier);
		map.put(PROPERTY_NODE_SOURCE, LABEL_RDA);
		map.put(PROPERTY_NODE_TYPE, LABEL_PARTY);
		
		addData(map, PROPERTY_TYPE, party.getType());
		addData(map, PROPERTY_DATE_MODIFIED, party.getDateModified());
		
		for (Object object : party.getIdentifierOrNameOrLocation()) {
			if (object instanceof IdentifierType) {
				IdentifierType identifier = (IdentifierType) object;
				String type = identifier.getType();
				if (null == type || type.isEmpty())
					type = PROPERTY_IDENTIFIER;
				else
					type = PROPERTY_IDENTIFIER + "_" + type;
				addData(map, type, identifier.getValue());
			} else if (object instanceof NameType) { 
				NameType name = (NameType) object;
				
				String lang = name.getLang();
				String dateFrom = name.getDateFrom();
				String dateTo = name.getDateTo();
				
				if (null == lang && null == dateFrom && null == dateTo) {
					String type = name.getType();
					if (null == type || type.isEmpty())
						type = PROPERTY_NAME;
					else
						type = PROPERTY_NAME + "_" + type;
					
					addData(map, type, getFullName(name));
				}
			} /*else if (object instanceof LocationType) {
				LocationType location = (LocationType) object;
			} */ else if (object instanceof SubjectType) {
				SubjectType subject = (SubjectType) object;
				
				String type = subject.getType();
				if (null == type || type.isEmpty())
					type = PROPERTY_SUBJECT;
				else
					type = PROPERTY_SUBJECT + "_" + type;
				
				addData(map, type, subject.getValue());		
			} else if (object instanceof DescriptionType) {
				DescriptionType description = (DescriptionType) object;

				String type = description.getType();
				if (null == type || type.isEmpty())
					type = PROPERTY_SUBJECT;
				else
					type = PROPERTY_SUBJECT + "_" + type;
				
				addData(map, type, description.getValue());		
			} else if (object instanceof RelatedObjectType) {
				RelatedObjectType relatedObject = (RelatedObjectType) object;
				for (RelationType relType : relatedObject.getRelation()) {
					RelatedObject relObject = new RelatedObject();
					
					relObject.key = key;
					relObject.relatedKey = relatedObject.getKey();
					relObject.relationshipName = relType.getType();
					
					for (Object descriptionOrUrl : relType.getDescriptionOrUrl()) {
						if (descriptionOrUrl instanceof RelationType.Description) 
							relObject.addDescription(((RelationType.Description)descriptionOrUrl).getValue());
						else if (descriptionOrUrl instanceof String)
							relObject.addUrl((String) descriptionOrUrl);
					}
					
					relatedObjects.add(relObject);
				}				
			}			
		}
		
		RestNode node = graphDb.getOrCreateNode(indexRDAParty, PROPERTY_KEY, key, map);
		if (!node.hasLabel(Labels.Party))
			node.addLabel(Labels.Party); 
		if (!node.hasLabel(Labels.RDA))
			node.addLabel(Labels.RDA);
	}
	
	@SuppressWarnings("unchecked")
	private void addData(Map<String, Object> map, String field, String data) {
		if (null != field && null != data && !data.isEmpty()) {
			Object par = map.get(field);
			if (null == par) 
				map.put(field, data);
			else {
				if (par instanceof String) {
					if (((String)par).equals(data))
						return; // we already have this string
					
					Set<String> pars = new HashSet<String>();
					pars.add((String) par);
					pars.add(data);
					map.put(field, pars);
				} else 
					((Set<String>)par).add(data);
			}
		}
	}
	
	private String getFullName(NameType name) {
		String family = null;
		String given = null;
		String title = null;
		String suffix =  null;
		
		for (NameType.NamePart part : name.getNamePart()) {
			final String type = part.getType();
			if (null != type && !type.isEmpty()) {
				if (type.equals(NAME_PART_FAMILY))
					family = part.getValue();
				else if (type.equals(NAME_PART_GIVEN))
					given = part.getValue();
				else if (type.equals(NAME_PART_TITLE))
					title = part.getValue();
				else if (type.equals(NAME_PART_SUFFIX))
					suffix = part.getValue();
			}				
		}
		
		StringBuilder sb = new StringBuilder();
		if (null != title && !title.isEmpty()) 
			sb.append(title);
		if (null != suffix && !suffix.isEmpty()) {
			if (sb.length() > 0)
				sb.append(" ");
			
			sb.append(suffix);
		}
		if (null != given && !given.isEmpty()) {
			if (sb.length() > 0)
				sb.append(" ");
			
			sb.append(given);
		}
		if (null != family && !family.isEmpty()) {
			if (sb.length() > 0)
				sb.append(" ");
			
			sb.append(family);
		}
		for (NameType.NamePart part : name.getNamePart()) {
			final String type = part.getType();
			if (null != type 
					&& !type.isEmpty() 
					&& (type.equals(NAME_PART_FAMILY) 
							|| type.equals(NAME_PART_GIVEN) 
							|| type.equals(NAME_PART_TITLE) 
							|| type.equals(NAME_PART_SUFFIX)))
				continue;
			
			if (sb.length() > 0)
				sb.append(" ");
			
			sb.append(part.getValue());				
		}
		
		return sb.toString();
	}
	
	private String getFullDate(DatesType dates) {
		String dateFrom = null;
		String dateTo = null;
		
		for (DatesType.Date date : dates.getDate()) {
			final String type = dates.getType();
			if (null != type && !type.isEmpty()) {
				if (type.equals(DATE_FROM))
					dateFrom = date.getValue();
				else if (type.equals(DATE_TO))
					dateTo = date.getValue();
			}				
		}
		
		StringBuilder sb = new StringBuilder();
		if (null != dateFrom && !dateFrom.isEmpty()) 
			sb.append(dateFrom);
		if (null != dateTo && !dateTo.isEmpty()) {
			if (sb.length() > 0)
				sb.append(" - ");
			
			sb.append(dateTo);
		}
		
		return sb.toString();
	}
	
	private RestNode findRDADatasetByKey(final String key) {
		if (null != key && !key.isEmpty()) {
			Map<String, Object> pars = new HashMap<String, Object>();
			pars.put("key", key);
			
			QueryResult<Map<String, Object>> nodes = engine.query("MATCH (n:RDA) WHERE has(n.key) and n.key={key} RETURN n", pars);
			for (Map<String, Object> row : nodes) {
				RestNode node = (RestNode) row.get("n");
				if (null != node) 
					return node;
			}
		}
		
		return null;
	}
	
	private void createUniqueRelationship(RestNode nodeStart, RestNode nodeEnd, 
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
		
		++createdRelationships;
	}
}
