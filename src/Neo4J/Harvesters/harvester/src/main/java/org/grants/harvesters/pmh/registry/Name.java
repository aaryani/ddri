package org.grants.harvesters.pmh.registry;

import java.util.ArrayList;
import java.util.List;

import org.grants.harvesters.Harvester;
import org.w3c.dom.Element;

public class Name {
	
	public enum Type {
		unknown, primary, abbreviated, alternative, text, formerly
	}
	
	public Type type;
	public String typeString;
	public String dateFrom;
	public String dateTo;	
	
	public List<NamePart> parts;

	public void setTypeString(final String type) {
		this.typeString = type;
		if (null == type || type.length() == 0)
			this.type = Type.unknown;
		else if (type.equals("alternate"))
			this.type = Type.abbreviated;
		else
			this.type = Type.valueOf(type);
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
	
	public NamePart getNamePartByType(NamePart.Type type) {
		if (null != parts)
			for (NamePart part : parts) 
				if (part.type == type)
					return part;
		
		return null;
	}
	
	public String getFullName() {
		NamePart full = getNamePartByType(NamePart.Type.full); 
		if (null != full)
			return full.value;		
		return null;		
	}
	
	public String getPersonName() {
		NamePart family = getNamePartByType(NamePart.Type.family); 
		NamePart given = getNamePartByType(NamePart.Type.given); 
		if (null != family && null != given) {
			NamePart suffix = getNamePartByType(NamePart.Type.suffix); 
			NamePart title = getNamePartByType(NamePart.Type.title); 
			
			String name = null;
			if (null != suffix && suffix.IsValid())
				name = suffix.value;
			if (null != title && title.IsValid()) {
				if (null != name)
					name += ' ';
				name += title.value;
			}
			if (null != given && given.IsValid()) {
				if (null != name)
					name += ' ';
				name += given.value;
			}
			if (null != given && given.IsValid()) {
				if (null != name)
					name += ' ';
				name += given.value;
			}
			
			return name;
		}  		
		return null;
	}
	
	public String GetCombinedName() {
		StringBuilder sb = new StringBuilder();
		for (NamePart part : parts) 
			if (part.IsValid()) {
				if (sb.length() == 0)
					sb.append(' ');
				sb.append(part.value);
			}
		if (sb.length() > 0)
			return sb.toString();		
		return null;
	}
	
	public String GetActivityName() {
		return GetCombinedName();		
	}
	
	public String GetCollectionName() {
		return GetCombinedName();		
	}
	
	public String GetServiceName() {
		return GetCombinedName();		
	}
	
	public String GetPartyName() {
		String name = getPersonName();
		if (null == name)
			name = getFullName();
		if (null == name)
			name = GetCombinedName();		
		return name;
	}
	
}
