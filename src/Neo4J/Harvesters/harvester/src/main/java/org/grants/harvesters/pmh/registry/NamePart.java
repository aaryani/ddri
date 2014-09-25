package org.grants.harvesters.pmh.registry;

import org.w3c.dom.Element;

public class NamePart {

	public enum Type {
	    unknown,
		family, 		// last name or surname
	    given, 			// forename or given or Christian name
	    suffix, 		// honours, awards, qualifications and other identifiers conferred
	    title, 			// word or phrase indicative of rank, office, nobility, honour, etc., or a term of address associated with a person
	    superior,		// part of a name that describes a party (group) that contains one or more integral subordinate parties (sub-groups or sub-units).
	    subordinate, 	// part of a name that describes a party (group) that is an integral sub-group or sub-unit of a superior party (group).
	    full,
	    extension,
	    primary,
	    text,
	    jurisdiction,
	    parent    
	}
	
	public Type type;
	public String typeString;
	public String value;
	
	public void setTypeString(final String type) {
		this.typeString = type;
		try {
			if (null == type || type.length() == 0)
				this.type = Type.unknown;
			else if (type.equals("familyname"))
				this.type = Type.family;
			else if (type.equals("givenname"))
				this.type = Type.given;
			else
				this.type = Type.valueOf(type);
		}
		catch(Exception e) {
			System.out.println("Invalid Part Name type: " + type);
			
			this.type = Type.unknown;
		}
	}
	
	public static NamePart fromElement(Element element) {
		NamePart namePart = new NamePart();
		
		namePart.setTypeString(element.getAttribute("type").trim());
		namePart.value = element.getTextContent();
		
		return namePart;
	}
	
	public boolean IsValid() {
		return null != value && !value.isEmpty(); 
	}
}
