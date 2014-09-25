package org.grants.harvesters.pmh.registry;

import org.w3c.dom.Element;

public class AddressPart {
	public enum Type {
		unknwown,
		addressLine, 		// an address part that is a separate line of a structured address
	    text, 				// a single address part that contains the whole address in unstructured form
	    telephoneNumber, 	// an address part that contains a telephone number including a mobile telephone number
	    faxNumber			// an address part that contains a fax (facsimile) number
	}
	
	public Type type;
	public String typeString;
	public String value;	
	
	public void setTypeString(final String type) {
		this.typeString = type;
		try {
			this.type = Type.valueOf(type);
		} catch(Exception e) {
			System.out.println("Invalid Address Part type: " + type);
			this.type = Type.unknwown;
		}
	}
		
	public static AddressPart fromElement(Element element) {
		AddressPart addressPart = new AddressPart();
		
		addressPart.setTypeString(element.getAttribute("type"));
		addressPart.value = element.getTextContent();
		
		return addressPart;
	}
	
	public boolean IsValid() {
		return null != value && !value.isEmpty();
	}
}
