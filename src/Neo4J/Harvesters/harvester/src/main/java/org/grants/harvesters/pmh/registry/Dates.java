package org.grants.harvesters.pmh.registry;

import java.util.ArrayList;
import java.util.List;

import org.grants.harvesters.Harvester;
import org.w3c.dom.Element;

public class Dates {
	protected static final String DC_AVALIABLE = "dc.available";
	protected static final String DC_CREATED = "dc_created";
	protected static final String DC_DATE_ACCEPTED = "dc.dateAccepted";
	protected static final String DC_DATE_SUBMITTED = "dc.dateSubmitted";
	protected static final String DC_ISSUED = "dc.issued";
	protected static final String DC_VALID = "dc.valid";
	public enum Type {
		unknown,
		available,		// dc.available: Date (often a range) that the resource became or will become available.
		created, 		// dc.created: Date of creation of the resource.
		accepted, 	// dc.dateAccepted: Date of acceptance of the resource.
		submitted, 	// dc.dateSubmitted: Date of submission of the resource.
		issued,			// dc.issued: Date of formal issuance (e.g.publication) of the resource.
		valid			// dc.valid: Date (often a range) of validity of a resource.
	}
	
	public Type type;
	public String typeString;
	public List<Date> dates;
	
	public void setTypeString(final String type) {
		this.typeString = type;
		
		try {
			if (null == type || type.isEmpty())
				this.type = Type.unknown;
			else if (type.equals(DC_AVALIABLE))
				this.type = Type.available;
			else if (type.equals(DC_CREATED))
				this.type = Type.created;
			else if (type.equals(DC_DATE_ACCEPTED))
				this.type = Type.accepted;
			else if (type.equals(DC_DATE_SUBMITTED))
				this.type = Type.submitted;
			else if (type.equals(DC_ISSUED))
				this.type = Type.issued;
			else if (type.equals(DC_VALID))
				this.type = Type.valid;
			else
				this.type = Type.valueOf(type);
		} catch (Exception e) {
			System.out.println("Invalid Dates Type: " + type);
			this.type = Type.unknown;
		}
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
