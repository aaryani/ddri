package org.grants.harvesters.pmh.registry;

import org.w3c.dom.Element;

public class Subject {
	public String typeString;
	public String termIdentifier;
	public String value;
	
	public static Subject fromElement(Element element) {
		Subject subject = new Subject();
		
		subject.typeString = element.getAttribute("type");
		subject.termIdentifier = element.getAttribute("termIdentifier");
		subject.value = element.getTextContent();
		
		return subject;
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
			
			if (null != termIdentifier && !termIdentifier.isEmpty()) {
				sb.append(" (");
				sb.append(termIdentifier);
				sb.append(')');
			}
			
			return sb.toString();
		}
		
		return null;		
	}
}
