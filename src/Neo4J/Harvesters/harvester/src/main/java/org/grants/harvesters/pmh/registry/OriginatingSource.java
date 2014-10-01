package org.grants.harvesters.pmh.registry;

import org.w3c.dom.Element;

public class OriginatingSource {
	private static final String AUTHORATIVE = "authorative";
	public enum Type {
		unknown, authoritative
	}
	
	public Type type;
	public String typeString;
	public String value;
	
	public void setTypeString(final String type) {
		this.typeString = type;
		try {
			if (null == type || type.isEmpty())
				this.type = Type.unknown;
			else if (type.equals(AUTHORATIVE))
				this.type = Type.authoritative;
			else
				this.type = Type.valueOf(type);
		} catch (Exception e) {
			System.out.println("Invalid Originating Source Type: " + type);
			
			this.type = Type.unknown;
		}
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
