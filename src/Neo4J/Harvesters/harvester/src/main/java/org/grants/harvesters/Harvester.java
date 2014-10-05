package org.grants.harvesters;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Harvester {
	
	protected static Document GetXml( final String uri ) throws ParserConfigurationException, SAXException, IOException {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

	    dbf.setNamespaceAware(true);
	    dbf.setExpandEntityReferences(false);
	    dbf.setXIncludeAware(true);
	    
	  //  dbf.setValidating(dtdValidate || xsdValidate);
		
		DocumentBuilder db = dbf.newDocumentBuilder(); 
		Document doc = db.parse(uri);
		
		return doc;
	}
	
	protected static void printDocument(Document doc, OutputStream out) 
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
	
	protected static void printElement(Element element, OutputStream out) 
			throws IOException, TransformerException {
	    TransformerFactory tf = TransformerFactory.newInstance();
	    Transformer transformer = tf.newTransformer();
	    transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
	    transformer.setOutputProperty(OutputKeys.METHOD, "xml");
	    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
	    transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
	    transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
	
	    transformer.transform(new DOMSource(element), 
	         new StreamResult(new OutputStreamWriter(out, "UTF-8")));
	}
	
	public static Element getChildElementByTagName(Element parent, String tagName) {
	    for(Node child = parent.getFirstChild(); child != null; child = child.getNextSibling())
	        if(child instanceof Element && tagName.equals(((Element) child).getTagName())) 
	        	return (Element) child;
	    return null;
	}
	
	public static List<Element> getChildElementsByTagName(Element parent, String tagName) {
		List<Element> elements = null;
		for(Node child = parent.getFirstChild(); child != null; child = child.getNextSibling())
	        if(child instanceof Element && tagName.equals(((Element) child).getTagName())) {
	        	if (null == elements)
	        		elements = new ArrayList<Element>();
	        	elements.add((Element) child);
	        }
	    return elements;
	}
	
	public static Element getDocumentElementByTagName(Document doc, String tagName) {
		NodeList list = doc.getElementsByTagName(tagName);
		if (null != list)
			for (int i = 0; i < list.getLength(); ++i) {
				Node node = list.item(i);
				if (node instanceof Element)
					return (Element) node;		
			}
		return null;
	}
	
	public static String getChildElementTextByTagName(Element parent, String tagName) {
		Element element = getChildElementByTagName(parent, tagName);
		if (null != element)
			return element.getTextContent();
		return null;
	}
}
