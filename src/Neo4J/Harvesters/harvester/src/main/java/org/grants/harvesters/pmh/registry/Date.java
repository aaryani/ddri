package org.grants.harvesters.pmh.registry;

import org.w3c.dom.Element;

public class Date {
	public enum Type {
		unknown,
		dateFrom,
		dateTo
	}
	
	public Type type;
	public String typeString;
	public String dateFormat;
	public String value;
	
	public void setTypeString(final String type) {
		this.typeString = type;
		try {
			if (null == type || type.length() == 0)
				this.type = Type.unknown;
			else
				this.type = Type.valueOf(type);
		}
		catch(Exception e) {
			System.out.println("Invalid date type: " + type);
			this.type = Type.unknown;
		}
	}
	
	public static Date fromElement(Element element) {
		Date date = new Date();
		
		date.setTypeString(element.getAttribute("type"));
		date.dateFormat = element.getAttribute("dateFormat");
		date.value = element.getTextContent();
		
		return date;
	}

}
