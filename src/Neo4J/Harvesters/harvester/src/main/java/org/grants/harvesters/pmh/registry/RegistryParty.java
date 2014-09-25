package org.grants.harvesters.pmh.registry;

import org.w3c.dom.Element;

public class RegistryParty extends RegistryObject {

	public enum Type {
		unknown, group, person, administrativePosition, publisher
	}
	
	public Type type;
	public String typeString;
	public String dateModified;
	
	public void setTypeString(final String type) {
		this.typeString = type;
		try {
			if (null == type || type.isEmpty())
				this.type = Type.unknown;
			else if (type.equals("Group"))
				this.type = Type.group;
			else if (type.equals("Person") || type.equals("PERSON"))
				this.type = Type.person;
			else
				this.type = Type.valueOf(type);
		}
		catch (Exception e) {
			System.out.println("Invalid Registry Party type: " + type);
			
			this.type = Type.unknown;
		}
	}
	
	public static RegistryParty fromElement(Element element) {
		RegistryParty registryObject = new RegistryParty();
		
		registryObject.setTypeString(element.getAttribute("type").trim());
		registryObject.dateModified = element.getAttribute("dateModified");
		
		registryObject.initFromElement(element);

		return registryObject;
	}
}
