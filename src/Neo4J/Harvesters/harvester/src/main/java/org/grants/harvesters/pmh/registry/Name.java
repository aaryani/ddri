package org.grants.harvesters.pmh.registry;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.grants.harvesters.Harvester;
import org.w3c.dom.Element;

public class Name {
	private static final String ALTERNATE = "alternate";
	public enum Type {
		unknown, primary, abbreviated, alternative, text, formerly
	}
	
	public Type type;
	public String typeString;
	public String dateFrom;
	public String dateTo;	
	
	public List<NamePart> parts;

	protected void setTypeString(final String type) {
		this.typeString = type;
		try {
			if (null == type || type.isEmpty())
				this.type = Type.unknown;
			else if (type.equals(ALTERNATE))
				this.type = Type.alternative;
			else
				this.type = Type.valueOf(type);
		} catch(Exception e) {
			System.out.println("Invalid name type: " + type);
			
			this.type = Type.unknown;
		}
	}
	
	protected void setParts(List<Element> list) {
		if (null != list && list.size() > 0) {
			parts = new ArrayList<NamePart>();
			for (Element element : list)
				parts.add(NamePart.fromElement(element));
		}			
	}
	
	public static Name fromElement(Element element) {
		Name name = new Name();
		
		name.setTypeString(element.getAttribute("type"));
		name.dateFrom = element.getAttribute("dateFrom");
		name.dateTo = element.getAttribute("dateTo");
		name.setParts(Harvester.getChildElementsByTagName(element, "namePart"));
		
		return name;		
	}
	
	public boolean isValid() {
		if (null != parts && parts.size() > 0)
			for (NamePart part : parts) 
				if (part.isValid())
					return true;		
		return false;
	}
	
	protected static List<String> extractNameParts(List<NamePart> parts, NamePart.Type type) {
		List<String> list = new ArrayList<String>();
		Iterator<NamePart> iter = parts.iterator();
		while (iter.hasNext()) {
			NamePart part = iter.next();
			if (part.type == type) {
				if (part.isValid()) 
					list.add(part.toString());								
				iter.remove();
			}
		}	
		return list;
	}
	
	public List<String> getNameParts(NamePart.Type type) {
		List<String> list = null;
		if (null != parts)
			for (NamePart part : parts) 
				if (part.isValid() && part.type == type) {
					if (null == list) 
						list = new ArrayList<String>();				
					list.add(part.toString());
					
				}		
		return list;
	}
	
	public String getNamePart(NamePart.Type type) {
		if (null != parts)
			for (NamePart part : parts) 
				if (part.type == type)
					return part.toString();
		
		return null;
	}
	
	public String toString() {
		List<NamePart> parts = new ArrayList<NamePart>(this.parts);
		List<String> name = new ArrayList<String>();
		name.addAll(extractNameParts(parts, NamePart.Type.suffix));
		name.addAll(extractNameParts(parts, NamePart.Type.title));
		name.addAll(extractNameParts(parts, NamePart.Type.given));
		name.addAll(extractNameParts(parts, NamePart.Type.family));
		name.addAll(extractNameParts(parts, NamePart.Type.full));
		for (NamePart part : parts)
			if (part.isValid())
				name.add(part.toString());
	
		if (name.size() > 0)
			return StringUtils.join(name, ' ');
		
		return null;
	}
}
