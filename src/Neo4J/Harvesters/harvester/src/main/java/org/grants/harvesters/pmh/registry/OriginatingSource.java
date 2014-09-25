package org.grants.harvesters.pmh.registry;

import org.w3c.dom.Element;

public class OriginatingSource {
	public enum Type {
		unknown, authoritative
	}
	
	public Type type;
	public String typeString;
	public String value;
	
	public void setTypeString(final String type) {
		this.typeString = type;
		if (null == type || type.length() == 0)
			this.type = Type.unknown;
		else if (type.equals("authorative"))
			this.type = Type.authoritative;
		else
			this.type = Type.valueOf(type);
	}
	
	public boolean IsValid() {
		return null != value && !value.isEmpty();
	}
	
	public static OriginatingSource fromElement(Element element) {
		OriginatingSource originatingSource = new OriginatingSource();
		
		originatingSource.setTypeString(element.getAttribute("type"));
		originatingSource.value = element.getTextContent();
		
		return originatingSource;
	}
}
