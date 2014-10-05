package org.grants.harvesters.pmh;

import java.io.Serializable;

public class Record implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3957349468305765200L;
	
/*	public static final String TAG_RECORD = "record";
	public static final String TAG_SET = "set";
	public static final String TAG_KEY = "key";
	public static final String TAG_DATE = "date";
	public static final String TAG_FILE = "file";*/
	
	public String set;
	public String key;
	public String date;
//	public String file;
	
	public boolean isValid() {
		return !set.isEmpty() && !key.isEmpty(); // && !file.isEmpty();
	}
	
	
	/*public String getFileName(boolean dated) throws UnsupportedEncodingException {
		if (!dated && null != file && !file.isEmpty()) 
			return file;
		
		StringBuilder sb = new StringBuilder();
		sb.append(URLEncoder.encode(set, "UTF-8"));
		sb.append("/");
		sb.append(URLEncoder.encode(key, "UTF-8"));
		if (dated) {
			sb.append("_");
			sb.append(date);
		}
		sb.append(".xml");
		
		if (!dated) 
			return file = sb.toString();
		
		return sb.toString();		
	}*/
	/*
	public static Record fromElement(Element element) throws Exception {
		Record record = new Record();
		
		for(Node child = element.getFirstChild(); child != null; child = child.getNextSibling())
	        if(child instanceof Element) {
	        	String tagName = ((Element) child).getTagName();
	        	if (null != tagName) {
	        		if (tagName.equals(TAG_SET)) 
	        			record.set = child.getTextContent();
	        		else if (tagName.equals(TAG_KEY)) 
	        			record.key = child.getTextContent();
	        		else if (tagName.equals(TAG_DATE)) 
	        			record.date = child.getTextContent();
	        		else if (tagName.equals(TAG_FILE)) 
	        			record.file = child.getTextContent();
	        	}
	        }	
	    
		if (!record.isValid())
			throw new Exception("Invalid record");
		
		return record;
	}
	
	public Element toElement(Document doc) {
		Element record = doc.createElement(TAG_RECORD);
		
		Element set = doc.createElement(TAG_SET);
		set.appendChild(doc.createTextNode(this.set));
		record.appendChild(set);
		
		Element key = doc.createElement(TAG_KEY);
		key.appendChild(doc.createTextNode(this.key));
		record.appendChild(key);
		
		Element date = doc.createElement(TAG_DATE);
		date.appendChild(doc.createTextNode(this.date));
		record.appendChild(date);
		
		Element file = doc.createElement(TAG_FILE);
		file.appendChild(doc.createTextNode(this.file));
		record.appendChild(file);
		
		return record;
	}*/
	
	
}
