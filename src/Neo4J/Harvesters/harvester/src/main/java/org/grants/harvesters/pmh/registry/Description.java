package org.grants.harvesters.pmh.registry;

import org.w3c.dom.Element;

public class Description {
	public String typeString;
	public String value;
	
	public static Description fromElement(Element element) {
		Description description = new Description();
		
		description.typeString = element.getAttribute("type");
		description.value = element.getTextContent();
		
		return description;
	}
	
	public boolean isValid() {
		return null != value && !value.isEmpty();
	}
	
	public String toString() {
		if (isValid()) {
			StringBuilder sb = new StringBuilder();
			
			if (null != typeString && !typeString.isEmpty()) {
				sb.append(typeString);
				sb.append(':');
			}
			
			sb.append(value);
		
			return sb.toString();
		}
		
		return null;		
	}
}
