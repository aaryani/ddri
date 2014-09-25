package org.grants.harvesters.pmh.registry;

import java.util.ArrayList;
import java.util.List;

import org.grants.harvesters.Harvester;
import org.w3c.dom.Element;

public class Dates {
	public enum Type {
		unknown,
		available,		// dc.available: Date (often a range) that the resource became or will become available.
		created, 		// dc.created: Date of creation of the resource.
		dateAccepted, 	// dc.dateAccepted: Date of acceptance of the resource.
		dateSubmitted, 	// dc.dateSubmitted: Date of submission of the resource.
		issued,			// dc.issued: Date of formal issuance (e.g.publication) of the resource.
		valid			// dc.valid: Date (often a range) of validity of a resource.
	}
	
	public Type type;
	public String typeString;
	public List<Date> dates;
	
	public void setTypeString(final String type) {
		this.typeString = type;
		if (null == type || type.length() == 0)
			this.type = Type.unknown;
		else if (type.equals("dc.available"))
			this.type = Type.available;
		else if (type.equals("dc.created"))
			this.type = Type.created;
		else if (type.equals("dc.dateAccepted"))
			this.type = Type.dateAccepted;
		else if (type.equals("dc.dateSubmitted"))
			this.type = Type.dateSubmitted;
		else if (type.equals("dc.issued"))
			this.type = Type.issued;
		else if (type.equals("dc.valid"))
			this.type = Type.valid;
		else
			this.type = Type.valueOf(type);
	}
	
	/*
	public void addDate(Date date) {
		if (null == dates)
			dates = new ArrayList<Date>();
		
		dates.add(date);		
	}
	*/
	
	protected void setDates(List<Element> list) {
		if (null != list && list.size() > 0) {
			dates = new ArrayList<Date>();
			for (Element element : list)
				dates.add(Date.fromElement(element));
		}
	}	
	
	
	public static Dates fromElement(Element element) {
		Dates dates = new Dates();
		
		dates.setTypeString(element.getAttribute("type"));
		dates.setDates(Harvester.getChildElementsByTagName(element, "date"));
		
		return dates;
	}

}
