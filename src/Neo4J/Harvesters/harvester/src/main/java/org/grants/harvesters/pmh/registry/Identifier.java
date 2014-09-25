package org.grants.harvesters.pmh.registry;

import org.w3c.dom.Element;

public class Identifier {
	public String typeString;
	public String value;
	
	public static Identifier fromElement(Element element) {
		Identifier identifier = new Identifier();
		
		identifier.typeString = element.getAttribute("type");
		identifier.value = element.getTextContent();
		
		return identifier;
	}
}
