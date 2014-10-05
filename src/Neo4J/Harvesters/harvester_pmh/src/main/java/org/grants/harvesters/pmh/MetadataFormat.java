package org.grants.harvesters.pmh;

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class MetadataFormat {
	
	public static final String TAG_METADATA_FORMAT = "metadataFormat";
	public static final String TAG_METADATA_PREFIX = "metadataPrefix";
	public static final String TAG_SCHEMA = "schema";
	public static final String TAG_METADATA_NAMESPACE = "metadataNamespace";
	
	public String metadataPrefix;
	public String schema;
	public String metadataNamespace;
	
	public static MetadataFormat fromElement(Element element) {
		MetadataFormat metadataFormat = new MetadataFormat();
		
		for(Node child = element.getFirstChild(); child != null; child = child.getNextSibling())
	        if(child instanceof Element) {
	        	String tagName = ((Element) child).getTagName();
	        	if (null != tagName) {
	        		if (tagName.equals(TAG_METADATA_PREFIX)) 
	        			metadataFormat.metadataPrefix = child.getTextContent();
	        		else if (tagName.equals(TAG_SCHEMA)) 
	        			metadataFormat.schema = child.getTextContent();
	        		else if (tagName.equals(TAG_METADATA_NAMESPACE)) 
	        			metadataFormat.metadataNamespace = child.getTextContent();
	        	}
	        }	
	
		return metadataFormat;
	}
	
	public static List<MetadataFormat> getMetadataFormats(Document doc) {
		List<MetadataFormat> metadataFormats = new ArrayList<MetadataFormat>();
		
		NodeList list = doc.getElementsByTagName(TAG_METADATA_FORMAT);
		if (null != list)
			for (int i = 0; i < list.getLength(); ++i) {
				Node node = list.item(i);
				if (node instanceof Element) 
					metadataFormats.add(fromElement((Element) node));
					
			}
		
		return metadataFormats;		
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(TAG_METADATA_FORMAT);
		sb.append(": [");
		sb.append(TAG_METADATA_PREFIX);
		sb.append("=");
		sb.append(metadataPrefix);
		sb.append(", ");
		sb.append(TAG_SCHEMA);
		sb.append("=");
		sb.append(schema);
		sb.append(", ");
		sb.append(TAG_METADATA_NAMESPACE);
		sb.append("=");
		sb.append(metadataNamespace);
		sb.append("]");
		
		return sb.toString();
	}
}
