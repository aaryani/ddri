package org.grants.importers.dc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.openarchives.oai._2.StatusType;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class App {

	private static enum Labels implements Label {
		Publication, Cern
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
		if (null != field && !field.isEmpty() && null != data && !data.isEmpty()) {
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
		RestAPI graphDb = new RestAPIFacade("http://localhost:7476/db/data/");  
		RestCypherQueryEngine engine=new RestCypherQueryEngine(graphDb);  
		
		engine.query("CREATE CONSTRAINT ON (n:Cern_Publication) ASSERT n.doi IS UNIQUE", Collections.<String, Object> emptyMap());
		RestIndex<Node> index = graphDb.index().forNodes("Cern_Publication");
		
		try {
			JAXBContext jc = JAXBContext.newInstance( "org.openarchives.oai._2:org.purl.dc.elements._1" );
			Unmarshaller u = jc.createUnmarshaller();
			
			File[] folders = new File("cern/xml/oai_dc").listFiles();
			for (File folder : folders)
				if (folder.isDirectory() && !folder.getName().equals("_cache")) {
					File[] files = folder.listFiles();
					for (File file : files)  
						if (!file.isDirectory()) {				
							try {
								JAXBElement<?> element = (JAXBElement<?>) u.unmarshal( new FileInputStream( file ) );
								
								RecordType record = (RecordType) element.getValue();	
								HeaderType header = record.getHeader();
								
								if (header.getStatus() != StatusType.DELETED) {
									
									String idetifier = header.getIdentifier();
									System.out.println("idetifier: " + idetifier);
	//								System.out.println(idetifier.toString());
								//	String datestamp = header.getDatestamp();
		//							System.out.println(datestamp.toString());
								//	List<String> specs = header.getSetSpec();
									
									if (null != idetifier && !idetifier.isEmpty() && null != record.getMetadata()) {
										Element metadata = (Element) record.getMetadata().getAny();
										if (null != metadata) {
											Map<String, Object> map = new HashMap<String, Object>();
											
											for(org.w3c.dom.Node child = metadata.getFirstChild(); child != null; child = child.getNextSibling())
										        if(child instanceof Element) {
										        	String tag = ((Element) child).getLocalName();
										        	if (tag.equals("identifier")) {
										        		if (child.getTextContent().contains("cds.cern.ch"))
										        			addData(map, "cern_url", child.getTextContent());
										        	} else if (tag.equals("language")) {
										        		addData(map, "language", child.getTextContent());
										        	} else if (tag.equals("title")) {
										        		addData(map, "title", child.getTextContent());
										        	} else if (tag.equals("subject")) {
										        		addData(map, "subject", child.getTextContent());
										        	} else if (tag.equals("publisher")) {
										        		addData(map, "publisher", child.getTextContent());
										        	} else if (tag.equals("date")) {
										        		addData(map, "date", child.getTextContent());
										        	}
										        }
										
											map.put("doi", idetifier);
											map.put("node_source", "Cern");
											map.put("node_type", "Publication");
											
										//	System.out.println("Create node");
											
											RestNode node = graphDb.getOrCreateNode(index, "doi", idetifier, map);
											if (!node.hasLabel(Labels.Publication))
												node.addLabel(Labels.Publication); 
											if (!node.hasLabel(Labels.Cern))
												node.addLabel(Labels.Cern);
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
