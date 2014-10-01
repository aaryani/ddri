package org.grants.harvesters.pmh.registry;

import org.w3c.dom.Element;

public class RegistryActivity extends RegistryObject {
	
	private static final String PROJECT = "PROJECT";
	public enum Type {
		unknown, award, course, event, program, project, dataset
	}
	
	public Type type;
	public String typeString;
	public String dateModified;
	
	public void setTypeString(final String type) {
		typeString = type;
		try {
			if (type.equals(PROJECT))
				this.type = Type.project;
			else
				this.type = Type.valueOf(type);
		} catch (Exception e) {
			System.out.println("Invalid Registry Activity Type: " + type);
			this.type = Type.unknown;
		}
		
	}
	
	public static RegistryActivity fromElement(Element element) {
		RegistryActivity registryObject = new RegistryActivity();
		
		registryObject.setTypeString(element.getAttribute("type"));
		registryObject.dateModified = element.getAttribute("dateModified");
		
		registryObject.initFromElement(element);

		return registryObject;
	}

}
