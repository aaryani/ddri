package org.grants.harvesters.pmh;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.grants.harvesters.Harvester;
import org.grants.harvesters.pmh.registry.OriginatingSource;
import org.grants.harvesters.pmh.registry.Record;
import org.grants.harvesters.pmh.registry.RegistryActivity;
import org.grants.harvesters.pmh.registry.RegistryCollection;
import org.grants.harvesters.pmh.registry.RegistryObject;
import org.grants.harvesters.pmh.registry.RegistryParty;
import org.grants.harvesters.pmh.registry.RegistryService;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class HarvesterPmh extends Harvester {
	
	protected static final String URL_IDENTIFY = "?verb=Identify";
	protected static final String URL_LIST_SETS = "?verb=ListSets";
	protected static final String URL_LIST_RECORDS = "?verb=ListRecords&set=%s&metadataPrefix=%s";
	protected static final String URL_LIST_RECORDS_RESUMPTION_TOKEN = "?verb=ListRecords&resumptionToken=%s";

	protected static final String ELEMENT_ROOT = "OAI-PMH";
	
		
	protected enum MetadataPrefix {
		rif
	};
	
	protected final String repoUrl;
	
//	private String responseDate;
	private String repositoryName;
//	private String baseUrl; // no need base Url, as we have repoUrl instead
	private String protocolVersion;
	private String earliestTimestamp;
	private String deletedRecord;
	private String granularity;
	private String adminEmail;
	
	public HarvesterPmh( final String repoUrl ) {
		this.repoUrl = repoUrl;
	}
	
	public String getRepositoryName() { return repositoryName; }
	public String getProtocolVersion() { return protocolVersion; }
	public String getEarliestTimestamp() { return earliestTimestamp; }
	public String getDeletedRecordBehavior() { return deletedRecord; }
	public String getGranularityTemplate() { return granularity; }
	public String getAdminEmail() { return adminEmail; }	
	
	public boolean Identify() {
		String url =  repoUrl + URL_IDENTIFY;
		
		try {
			Document doc = GetXml(url);
			// printDocument(doc, System.out);
			Element root = getDocumentElementByTagName(doc, ELEMENT_ROOT);
			Element identify = getChildElementByTagName(root, "Identify");
				
			//responseDate = root.getElementsByTagName("responseDate").item(0).getTextContent();			

			repositoryName = getChildElementTextByTagName(identify, "repositoryName");
			protocolVersion = getChildElementTextByTagName(identify, "protocolVersion");
			earliestTimestamp = getChildElementTextByTagName(identify, "earliestTimestamp");
			deletedRecord = getChildElementTextByTagName(identify, "deletedRecord");
			granularity = getChildElementTextByTagName(identify, "granularity");
			adminEmail = getChildElementTextByTagName(identify, "adminEmail");
			
			return true;
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	public Map<String, String> ListSets() {
		String url =  repoUrl + URL_LIST_SETS;
		
		try {
			Document doc = GetXml(url);
			Element root = getDocumentElementByTagName(doc, ELEMENT_ROOT);
			List<Element> sets = getChildElementsByTagName(
					getChildElementByTagName(root, "ListSets"), "set");
			if (null == sets)
				throw new Exception("The sets collection is empty");
			Map<String, String> mapSets = null; 
			
			for (Element set : sets) {
				String setName = getChildElementTextByTagName(set, "setName");
				String setGroup = getChildElementTextByTagName(set, "setSpec");
				
				if (null == mapSets)
					mapSets = new HashMap<String, String>();
				
				if (mapSets.put(setGroup, setName) != null)
					throw new Exception("The group already exists in the set");
			}
			
			return mapSets;						
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TransformerException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}		
		
		return null;
	}
	
	public List<Record> ListRecords( final String set, final MetadataPrefix metadataPrefix, 
			final String resumptionToken, List<Record> recordList ) {
		String url = repoUrl;		 
		if (null != resumptionToken) {
			try {
				url += String.format(URL_LIST_RECORDS_RESUMPTION_TOKEN, URLEncoder.encode(resumptionToken, "UTF-8"));
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			}
		}
		else
			url += String.format(URL_LIST_RECORDS, set,  metadataPrefix.name());
		System.out.println(url);
		
		try {
			Document doc = GetXml(url);
			
			Element root = getDocumentElementByTagName(doc, ELEMENT_ROOT);
			
			if (!CheckForError(root))
				return recordList;
			
			/*	Transformer transformer = TransformerFactory.newInstance().newTransformer();
			Result output = new StreamResult(new File("output.xml"));
			Source input = new DOMSource(doc);

			transformer.transform(input, output);*/
			

		/*	try {
				printDocument(doc, System.out);
			} catch (TransformerException e) {
				e.printStackTrace();
			}	*/
			
			List<Element> records = getChildElementsByTagName(getChildElementByTagName(root, "ListRecords"), "record");
			if (null != records) {
				for (Element rec : records) {
					
					Record record = new Record();

					Element header = getChildElementByTagName(rec, "header");
					Element metadata = getChildElementByTagName(rec, "metadata");
					
					record.identifier = getChildElementTextByTagName(header, "identifier");
					record.datestamp = getChildElementTextByTagName(header, "datestamp");
					
					List<Element> setSpecs = getChildElementsByTagName(header, "setSpec");
					if (setSpecs != null) {
						for (Element setSpec : setSpecs) {
							String spec = setSpec.getTextContent();
							int pos = spec.indexOf(":");
							if (pos >= 0) 
								record.addSpec(spec.substring(0, pos), spec.substring(pos+1));
						}
					}
					
					List<Element> registryObjects = getChildElementsByTagName(getChildElementByTagName(metadata, "registryObjects"), "registryObject");
					if (null != registryObjects) {
						for (Element obj : registryObjects) {
							
						/*	try {
								printElement((Element) obj, System.out);
							} catch (TransformerException e) {
								e.printStackTrace();
							} */							
							
							RegistryObject registryObject = parseRegistryObject(obj);
							
							registryObject.key = getChildElementTextByTagName(obj, "key");
							registryObject.originatingSource = OriginatingSource.fromElement(getChildElementByTagName(obj, "originatingSource"));
							
							record.addObject(registryObject);
						}
					}
					
					if (null == recordList)
						recordList = new ArrayList<Record>();
					
					recordList.add(record);
				}
			}
			
			NodeList nl = doc.getElementsByTagName("resumptionToken");
			if (nl != null && nl.getLength() > 0)
			{
				System.out.println("ResumptionToken Detected: ");
				
			/*	try {
					printElement((Element) nl.item(0), System.out);
				} catch (TransformerException e) {
					e.printStackTrace();
				}*/
				
				Element token = (Element) nl.item(0);
				
				recordList = ListRecords(set, metadataPrefix, token.getTextContent(), recordList);			
			}
			
			
			/*try {
				printDocument(doc, System.out);
			} catch (TransformerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
		
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return recordList;
	}
		
	protected RegistryObject parseRegistryObject(Element objectElement) throws Exception {	
		Element element = getChildElementByTagName(objectElement, "activity");
		if (null != element)
			return RegistryActivity.fromElement(element);
		
		element = getChildElementByTagName(objectElement, "collection");
		if (null != element)
			return RegistryCollection.fromElement(element);
		
		element = getChildElementByTagName(objectElement, "party");
		if (null != element)
			return RegistryParty.fromElement(element);
		
		element = getChildElementByTagName(objectElement, "service");
		if (null != element)
			return RegistryService.fromElement(element);
				
		throw new Exception("Uknown object type");
	}
		
	protected boolean CheckForError(Element root) throws DOMException, Exception {
		NodeList nl = root.getElementsByTagName("error");
		if (null != nl && nl.getLength() > 0)
		{
			Node node = nl.item(0);
			if (node instanceof Element) {
				System.out.println(String.format("Error code: %s, message: %s", 
						((Element) node).getAttribute("code"), 
						((Element) node).getTextContent()));
			
				return false;
			} else
				throw new Exception("Unable to parse error information");
		}	
		
		return true;
	}
	
	public List<Record> GetRecords() throws Exception {
		
				
		System.out.println("Identifying...");
		
		if (!Identify())
			throw new Exception("Unable to Identify the service");
		
		System.out.println("Downloading sets list...");
				
		Map<String, String> mapSets = ListSets();
		if (null == mapSets )
			throw new Exception("The sets collection is empty");
		
		List<Record> recordList = null;
		
		// try to load whole database into memory
		for (Map.Entry<String, String> entry : mapSets.entrySet()) {
		    String set = entry.getKey();
		    String setName = entry.getValue();
		    
		    System.out.println("Processing set: " +  URLDecoder.decode(setName, "UTF-8"));
		    
		    recordList = ListRecords(set, MetadataPrefix.rif, null, recordList);
		    
		    if (null != recordList) {
		    	System.out.println("Retrieved " + recordList.size() + "records");
		    	
		    }
		}
		
		return recordList;
	}	
}
