package org.grants.harvesters.pmh.registry;

import java.util.ArrayList;
import java.util.List;

import org.grants.harvesters.Harvester;
import org.w3c.dom.Element;

public class Format {
	public List<Identifier> identifiers;
	
	protected void setIdentifiers(List<Element> list) {
		if (null != list && list.size() > 0) {
			identifiers = new ArrayList<Identifier>();
			for (Element element : list)
				identifiers.add(Identifier.fromElement(element));
		}
	}
	
	public static Format fromElement(Element element) {
		Format format = new Format();
		
		format.setIdentifiers(Harvester.getChildElementsByTagName(element, "identifer"));
		
		return format;
	}
}
