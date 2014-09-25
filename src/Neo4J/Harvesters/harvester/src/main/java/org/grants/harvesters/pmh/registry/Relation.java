package org.grants.harvesters.pmh.registry;

import org.grants.harvesters.Harvester;
import org.w3c.dom.Element;

public class Relation {
	public String typeString;
	public String description;
	public String url;
	
	
	public static Relation fromElement(Element element) {
		Relation relation = new Relation();
		
		relation.typeString = element.getAttribute("type");
		relation.description = Harvester.getChildElementTextByTagName(element, "description"); 
		relation.url = Harvester.getChildElementTextByTagName(element, "url"); 
		
		return relation;
	}
}
