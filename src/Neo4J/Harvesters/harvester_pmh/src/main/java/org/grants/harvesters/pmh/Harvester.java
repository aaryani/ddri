package org.grants.harvesters.pmh;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.FileUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Harvester {
	
	protected static final String URL_IDENTIFY = "?verb=Identify";
	protected static final String URL_LIST_METADATA_FORMATS = "?verb=ListMetadataFormats";
	protected static final String URL_LIST_SETS = "?verb=ListSets";
	protected static final String URL_LIST_RECORDS = "?verb=ListRecords&set=%s&metadataPrefix=%s";
	protected static final String URL_LIST_RECORDS_RESUMPTION_TOKEN = "?verb=ListRecords&resumptionToken=%s";

	protected static final String ELEMENT_ROOT = "OAI-PMH";
	
	protected final String repoUrl;
	protected final String folderXml;
	protected final String indexName;
	protected Map<String, Record> records;
	protected DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	
//	private String responseDate;
	private String repositoryName;
//	private String baseUrl; // no need base Url, as we have repoUrl instead
	private String protocolVersion;
	private String earliestTimestamp;
	private String deletedRecord;
	private String granularity;
	private String adminEmail;
	
	public Harvester( final String repoUrl, final String folderXml, final String indexName ) {
		this.repoUrl = repoUrl;
		this.folderXml = folderXml;
		this.indexName = indexName;
		
		new File(this.folderXml).mkdirs();
	}
	
	public String getRepositoryName() { return repositoryName; }
	public String getProtocolVersion() { return protocolVersion; }
	public String getEarliestTimestamp() { return earliestTimestamp; }
	public String getDeletedRecordBehavior() { return deletedRecord; }
	public String getGranularityTemplate() { return granularity; }
	public String getAdminEmail() { return adminEmail; }	
	
	protected Document GetXml( final String uri ) throws ParserConfigurationException, SAXException, IOException {
		dbf.setNamespaceAware(true);
	    dbf.setExpandEntityReferences(false);
	    dbf.setXIncludeAware(true);
	    
	  //  dbf.setValidating(dtdValidate || xsdValidate);
		
		DocumentBuilder db = dbf.newDocumentBuilder(); 
		Document doc = db.parse(uri);
		
		return doc;
	}
	
	protected void printDocument(Document doc, OutputStream out) 
			throws IOException, TransformerException {
	    TransformerFactory tf = TransformerFactory.newInstance();
	    Transformer transformer = tf.newTransformer();
	    transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
	    transformer.setOutputProperty(OutputKeys.METHOD, "xml");
	    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
	    transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
	    transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
	
	    transformer.transform(new DOMSource(doc), 
	         new StreamResult(new OutputStreamWriter(out, "UTF-8")));
	}

	
	protected Element getChildElementByTagName(Element parent, String tagName) {
	    for(Node child = parent.getFirstChild(); child != null; child = child.getNextSibling())
	        if(child instanceof Element && tagName.equals(((Element) child).getTagName())) 
	        	return (Element) child;
	    return null;
	}
	
	protected List<Element> getChildElementsByTagName(Element parent, String tagName) {
		List<Element> elements = null;
		for(Node child = parent.getFirstChild(); child != null; child = child.getNextSibling())
	        if(child instanceof Element && tagName.equals(((Element) child).getTagName())) {
	        	if (null == elements)
	        		elements = new ArrayList<Element>();
	        	elements.add((Element) child);
	        }
	    return elements;
	}
	
	protected Element getDocumentElementByTagName(Document doc, String tagName) {
		NodeList list = doc.getElementsByTagName(tagName);
		if (null != list)
			for (int i = 0; i < list.getLength(); ++i) {
				Node node = list.item(i);
				if (node instanceof Element)
					return (Element) node;		
			}
		return null;
	}
	
	protected String getChildElementTextByTagName(Element parent, String tagName) {
		Element element = getChildElementByTagName(parent, tagName);
		if (null != element)
			return element.getTextContent();
		return null;
	}
		
	protected boolean CheckForError(Element root) {
		NodeList nl = root.getElementsByTagName("error");
		if (null != nl && nl.getLength() > 0)
		{
			Node node = nl.item(0);
			if (node instanceof Element) 
				System.out.println(String.format("Error code: %s, message: %s", 
						((Element) node).getAttribute("code"), 
						((Element) node).getTextContent()));
			
			else 
				System.out.println("Error in DOM structire, unable to extract error information");
				
			return false;
		}	
		
		return true;
	}
	
	public boolean identify() {
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
	
	/*<ListMetadataFormats>
<metadataFormat>
            <metadataPrefix>marcxml</metadataPrefix>
            <schema>http://www.openarchives.org/OAI/1.1/dc.xsd</schema>
            <metadataNamespace>http://purl.org/dc/elements/1.1/</metadataNamespace>
        </metadataFormat>
        <metadataFormat>
            <metadataPrefix>oai_dc</metadataPrefix>
            <schema>http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd</schema>
            <metadataNamespace>http://www.loc.gov/MARC21/slim</metadataNamespace>
        </metadataFormat>
    </ListMetadataFormats>*/
	
	public List<MetadataFormat> listMetadataFormats() {
		String url =  repoUrl + URL_LIST_METADATA_FORMATS;
		
		try {
			return MetadataFormat.getMetadataFormats(GetXml(url));
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		return null;
	}
	
	public Map<String, String> listSets() {
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
	
	protected Record findRecord( final String set, final String key ) {
		return records.get(set + ":" + key);
	}
	
	protected void addRecord( Record record) {
		records.put(record.set + ":" + record.key, record);
	}
	
	public String downloadRecords( final String set, final MetadataPrefix metadataPrefix, 
			final String resumptionToken ) throws ParserConfigurationException, SAXException, 
			IOException, TransformerFactoryConfigurationError, TransformerException {
				
		// create set folder
		String setName = URLEncoder.encode(set, "UTF-8");
		String setPath = null; //getSetPath(setName);
		String setCachePath = null;
	//	new File(setPath).mkdirs();
		
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
		
		Document doc  = GetXml(url);
			
		Element root = getDocumentElementByTagName(doc, ELEMENT_ROOT);
		
		if (!CheckForError(root))
			return null;
			
		/*	
			String fileName = "xml/" + set;
			if (null != offset)
				fileName += "_" + offset;
			fileName +=  ".xml"; 
			
			transformer.transform(new DOMSource(doc), new StreamResult(new File(fileName)));*/
		Transformer transformer = TransformerFactory.newInstance().newTransformer();
			
		List<Element> records = getChildElementsByTagName(getChildElementByTagName(root, "ListRecords"), "record");
		if (null != records) 
			for (Element record : records) {				
				Element header = getChildElementByTagName(record, "header");
				String identifier = getChildElementTextByTagName(header, "identifier");
				String datestamp = getChildElementTextByTagName(header, "datestamp");
				
				Record rec = findRecord(set, identifier);
				if (null == rec || !rec.date.equals(datestamp)) {
					// the file missing in the index, or file date is different
					
					String keyName = URLEncoder.encode(identifier, "UTF-8");
					String fineName = getFileNme(keyName);
					
					if (null == setPath)
						new File(setPath = getSetPath(setName)).mkdirs();
					
					String filePath = setPath + "/" + fineName;
					
					if (rec != null) {
						// file is different, copy the old file into the cashe
						if (null == setCachePath) 
							new File(setCachePath = getCacheSetPath(setName)).mkdirs();
												
						FileUtils.copyFile(new File(filePath), 
										   new File(setCachePath + "/" + getCacheFileName(keyName, datestamp)), true);
					} else {
						rec = new Record();
						rec.set = set;
						rec.key = identifier;
					//	rec.file = setName + "/" + fineName;
						
						addRecord(rec);
					}
					
					rec.date = datestamp;
					
					transformer.transform(new DOMSource(record), new StreamResult(filePath));						
				}
			}			
			
		NodeList nl = doc.getElementsByTagName("resumptionToken");
		if (nl != null && nl.getLength() > 0)
		{
			Element token = (Element) nl.item(0);
			String tokenString = token.getTextContent();
			if (null != tokenString && !tokenString.isEmpty()) {
				String cursor = token.getAttribute("cursor");
				String size = token.getAttribute("completeListSize");
				
				System.out.println("ResumptionToken Detected. Cursor: " + cursor + ", size: " + size);
				
				return token.getTextContent();
			}
		}
		
		return null;
	}		
	
	protected String getIndexPath() {
		return folderXml + "/" + indexName + ".idx";
	}
	
	protected String getSetPath(final String setName) {
		return folderXml + "/" + setName;
	}
	
	protected String getCacheSetPath(final String setName) {
		return folderXml + "/_cache/" + setName;
	}
	
	protected String getFileNme(final String keyName)  {
		return keyName + ".xml";
	}
	
	protected String getCacheFileName(final String keyName, final String timestamp)  {
		return keyName + "_" + timestamp + ".xml";
	}
	
	
	/*
	protected String getSetPath(final String set) {
		return folderXml + "/index.xml";
	}
	
	
	
	protected String getRecordPath(Record record) throws UnsupportedEncodingException {
		return folderXml + "/" + record.getFileName(false);
	}
	
	protected String getRecordCachePath(Record record) throws UnsupportedEncodingException {
		return folderXml + "/cache/" + record.getFileName(true);
	}*/
	
	protected void initIndex() {
		records = new HashMap<String, Record>();
		
		try {
			FileInputStream f = new FileInputStream(getIndexPath());
			ObjectInputStream in = new ObjectInputStream(f);
			Record r = null;
			try {
				do {
					r = (Record) in.readObject();
					if (r != null)
						addRecord(r);
				} while (r != null);
			} finally {
				in.close();
				f.close();
			}
		} catch (FileNotFoundException e) {
			System.out.println("Index file do not exists, new index has been created");
		} catch (IOException e) {
		} catch (ClassNotFoundException e) {
			System.out.println("Invalid index format");
			e.printStackTrace();
		}		
	}
	
	protected void saveIndex() {
		
		try {
			FileOutputStream f = new FileOutputStream(getIndexPath());
			ObjectOutputStream out = new ObjectOutputStream(f);
			try {
				for (Record record : records.values()) {
				    out.writeObject(record);
				}
			} finally {
				out.close();
				f.close();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
	
	public void harvest(MetadataPrefix prefix) throws Exception {
		System.out.println("Initializing index...");
		
		initIndex();
		
		System.out.println("Identifying...");
	
		if (!identify())
			throw new Exception("Unable to Identify the service");
	
		System.out.println("Downloading sets list...");
			
		Map<String, String> mapSets = listSets();
		if (null == mapSets )
			throw new Exception("The sets collection is empty");
		
			// try to load whole database into memory
		for (Map.Entry<String, String> entry : mapSets.entrySet()) {
		    String set = entry.getKey();
		    String setName = entry.getValue();
		    
		    System.out.println("Processing set: " +  URLDecoder.decode(setName, "UTF-8"));
		    
		    String resumptionToken = null;
		    int nError = 0;
		    do {
		    	try {
		    		resumptionToken = downloadRecords(set, prefix, resumptionToken);		
		    		
		    		nError = 0;		    		
		    		saveIndex();
		    	}
		    	catch (Exception e) {
		    		if (++nError >= 10) {
		    			System.out.println("Too much errors has been detected, abort download");
		    			
		    			throw e;
		    		} else { 
		    			System.out.println("Error downloading data");
		    		
		    			e.printStackTrace();
		    		}
		    	}
		    	
		    } while (null != resumptionToken && !resumptionToken.isEmpty());		    
		}
	}
}
