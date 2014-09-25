package org.grants.harvesters.pmh.registry;

import java.util.ArrayList;
import java.util.List;

import org.grants.harvesters.Harvester;
import org.w3c.dom.Element;

public class RelatedObject {

	public String key;
	public List<Relation> relations;
	
		protected void setRelations(List<Element> list) {
		if (null != list && list.size() > 0) {
			relations = new ArrayList<Relation>();
			for (Element element : list)
				relations.add(Relation.fromElement(element));
		}
	}	
	
	public static RelatedObject fromElement(Element element) {
		RelatedObject relatedObject = new RelatedObject();
		
		relatedObject.key = Harvester.getChildElementTextByTagName(element, "key");
		relatedObject.setRelations(Harvester.getChildElementsByTagName(element ,"relation"));
		
		return relatedObject;
	}
}
