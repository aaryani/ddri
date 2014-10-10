package org.grants.importers.mets;

import gov.loc.mets.MdSecType;
import gov.loc.mets.MdSecType.MdWrap;
import gov.loc.mets.MdSecType.MdWrap.XmlData;
import gov.loc.mets.Mets;

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

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.index.RestIndex;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.RecordType;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


public class App {
	
	public static final String MD_TYPE_MODS = "MODS";

	private static enum Labels implements Label {
		Book, Protocol, Activity, Dataset, Map, Image, Article, Dryad
	};
	
	public static Element findXmlElement(Element xml, String name) {
		if (xml.getLocalName().equals(name))
			return xml;

		return null;
	}
	
	public static Element findXmlElement(List<Object> xmls, String name) {
		for (Object xml : xmls) 
			if (xml instanceof Element && ((Element) xml).getLocalName().equals(name))
				return (Element) xml;

		return null;
	}
	
	public static Element findXmlElement(NodeList xmls, String name) {
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
	
	public static List<Element> findXmlElements(List<Object> xmls, String name) {
		List<Element> list = null;		
		for (Object xml : xmls) 
			if (xml instanceof Element && ((Element) xml).getLocalName().equals(name)) {
				if (null == list)
					list = new ArrayList<Element>();
				
				list.add((Element) xml);
			}

		return list;
	}
	
	@SuppressWarnings("unchecked")
	public static void addData(Map<String, Object> map, String field, String data) {
		if (null != field && null != data) {
			Object par = map.get(field);
			if (null == par) 
				map.put(field, data);
			else if (par instanceof String) {
				List<String> pars = new ArrayList<String>();
				pars.add((String) par);
				pars.add(data);
				map.put(field, par);				
			} else 
				((List<String>)par).add(data);			
		}
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// connect to graph database
		RestAPI graphDb = new RestAPIFacade("http://localhost:7474/db/data/");  
		RestCypherQueryEngine engine=new RestCypherQueryEngine(graphDb);  
		
		engine.query("CREATE CONSTRAINT ON (n:Dryad_Book) ASSERT n.doi IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:Dryad_Protocol) ASSERT n.dryad_id IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:Dryad_Activity) ASSERT n.doi IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:Dryad_Dataset) ASSERT n.doi IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:Dryad_Map) ASSERT n.doi IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:Dryad_Image) ASSERT n.doi IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:Dryad_Article) ASSERT n.doi IS UNIQUE", Collections.<String, Object> emptyMap());
		
		Map<String, RestIndex<Node>> mapIndexes = new HashMap<String, RestIndex<Node>>();
		mapIndexes.put("Book", graphDb.index().forNodes("Dryad_Book"));
		mapIndexes.put("Protocol", graphDb.index().forNodes("Dryad_Protocol"));
		mapIndexes.put("Activity", graphDb.index().forNodes("Dryad_Activity"));
		mapIndexes.put("Dataset", graphDb.index().forNodes("Dryad_Dataset"));
		mapIndexes.put("Map", graphDb.index().forNodes("Dryad_Map"));
		mapIndexes.put("Image", graphDb.index().forNodes("Dryad_Image"));
		mapIndexes.put("Article", graphDb.index().forNodes("Dryad_Article"));
		
		try {
			JAXBContext jc = JAXBContext.newInstance( "org.openarchives.oai._2:gov.loc.mets" );
			Unmarshaller u = jc.createUnmarshaller();
			
			File[] folders = new File("dryad").listFiles();
			for (File folder : folders)
				if (folder.isDirectory() && !folder.getName().equals("_cache")) {
					File[] files = folder.listFiles();
					for (File file : files)  
						if (!file.isDirectory()) {				
							try {
								JAXBElement<?> element = (JAXBElement<?>) u.unmarshal( new FileInputStream( file ) );
								
								RecordType record = (RecordType) element.getValue();	
								HeaderType header = record.getHeader();
								
								String idetifier = header.getIdentifier();
//								System.out.println(idetifier.toString());
								String datestamp = header.getDatestamp();
	//							System.out.println(datestamp.toString());
								List<String> specs = header.getSetSpec();
								
								if (null != record.getMetadata()) {
									Object metadata = record.getMetadata().getAny();
									if (metadata instanceof Mets) {
										Mets mets = (Mets) metadata;
										for (MdSecType dmdSec : mets.getDmdSec()) {
			//								System.out.println(dmdSec.getID().toString());
											MdWrap mdWrap = dmdSec.getMdWrap();
				//							System.out.println(mdWrap.getMDTYPE().toString());
											
											if (mdWrap.getMDTYPE().equals(MD_TYPE_MODS)) {
																			
												XmlData xmlData = mdWrap.getXmlData();
												
												List<Object> xmlObjects = xmlData.getAny();
											/*	for (Object xmlObject : xmlObjects) {
													System.out.println(((Element) xmlObject).getLocalName().toString());
													System.out.println(xmlObject.getClass().toString());
												}*/
												
												Map<String, Object> map = new HashMap<String, Object>();
												String dryadId = null;
												List<Element> identifiers = findXmlElements(xmlObjects, "identifier");
												if (null != identifiers) {
													for (Element identifier : identifiers) {
														String type = identifier.getAttribute("type");
														String identifierString = identifier.getTextContent();
														if (null != identifierString && ! identifierString.isEmpty()) {
															if (null != type && !type.isEmpty()) 
																addData(map, "identifier_" +  type, identifierString);
															else if (null == dryadId && identifierString.contains("doi:"))  
																dryadId = identifierString;
														}														
													}
													
												if (null != dryadId) {
													map.put("doi", dryadId);
													//map.put("node_type", Labels.Grant.name());
													map.put("node_source", "dryad");
													
													List<Element> names = findXmlElements(xmlObjects, "name");
													if (null != names)
														for (Element name : names) {
															String roleString = null;
															String nameString = null;
														
															Element role = findXmlElement(name.getChildNodes(), "role");
															if (null != role) {
																Element roleTerm = findXmlElement(role.getChildNodes(), "roleTerm");
																if (null != roleTerm)
																	roleString = roleTerm.getTextContent();																
															}
															Element namePart = findXmlElement(name.getChildNodes(), "namePart");
															if (null != namePart) 
																nameString = namePart.getTextContent();
															
															if (null != roleString && null != nameString) 
																addData(map, roleString, nameString);
														}
												
													Element title = findXmlElement(xmlObjects, "titleInfo");
													if (null != title)
														addData(map, "title", title.getTextContent());
													Element description = findXmlElement(xmlObjects, "abstract");
													if (null != description)
														addData(map, "description", description.getTextContent());
													
													Element genre = findXmlElement(xmlObjects, "genre");
													if (null != genre) {
														String genreString = genre.getTextContent();
														if (genreString.equals("Book")) {
															map.put("node_type", "Book");
														
															RestNode node = graphDb.getOrCreateNode(mapIndexes.get("Book"), "doi", dryadId, map);
															if (!node.hasLabel(Labels.Book))
																node.addLabel(Labels.Book); 
															if (!node.hasLabel(Labels.Dryad))
																node.addLabel(Labels.Dryad);
														
														} else if (genreString.equals("protocol")) {
															map.put("node_type", "Protocol");
															
															RestNode node = graphDb.getOrCreateNode(mapIndexes.get("Protocol"), "doi", dryadId, map);
															if (!node.hasLabel(Labels.Protocol))
																node.addLabel(Labels.Protocol); 
															if (!node.hasLabel(Labels.Dryad))
																node.addLabel(Labels.Dryad);
														
														} else if (genreString.equals("Activity")) {
															map.put("node_type", "Activity");
															
															RestNode node = graphDb.getOrCreateNode(mapIndexes.get("Activity"), "doi", dryadId, map);
															if (!node.hasLabel(Labels.Activity))
																node.addLabel(Labels.Activity); 
															if (!node.hasLabel(Labels.Dryad))
																node.addLabel(Labels.Dryad);
														
														} else if (genreString.equals("Dataset") || genreString.equals("dataset")) {
															map.put("node_type", "Dataset");
															
															RestNode node = graphDb.getOrCreateNode(mapIndexes.get("Dataset"), "doi", dryadId, map);
															if (!node.hasLabel(Labels.Dataset))
																node.addLabel(Labels.Dataset); 
															if (!node.hasLabel(Labels.Dryad))
																node.addLabel(Labels.Dryad);
														
														} else if (genreString.equals("Map") ) { 
															map.put("node_type", "Map");
															
															RestNode node = graphDb.getOrCreateNode(mapIndexes.get("Map"), "doi", dryadId, map);
															if (!node.hasLabel(Labels.Map))
																node.addLabel(Labels.Map); 
															if (!node.hasLabel(Labels.Dryad))
																node.addLabel(Labels.Dryad);
															
														} else if (genreString.equals("Image") ) { 
															map.put("node_type", "Image");
															
															RestNode node = graphDb.getOrCreateNode(mapIndexes.get("Image"), "doi", dryadId, map);
															if (!node.hasLabel(Labels.Image))
																node.addLabel(Labels.Image); 
															if (!node.hasLabel(Labels.Dryad))
																node.addLabel(Labels.Dryad);
														} else if (genreString.equals("Article") ) { 
															map.put("node_type", "Article");
															
															RestNode node = graphDb.getOrCreateNode(mapIndexes.get("Article"), "doi", dryadId, map);
															if (!node.hasLabel(Labels.Article))
																node.addLabel(Labels.Article); 
															if (!node.hasLabel(Labels.Dryad))
																node.addLabel(Labels.Dryad);
														}
													}
												}
											}											 
										}
									}
								}
								}
							
							} catch (FileNotFoundException e) {
								e.printStackTrace();
							}
						}
			}
			
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	/*	
		System.out.println("Done!");
		for (String genre : genres) {
			System.out.println(genre);

		}*/
		
	}

}
