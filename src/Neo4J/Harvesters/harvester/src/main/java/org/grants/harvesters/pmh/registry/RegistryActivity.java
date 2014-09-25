package org.grants.harvesters.pmh.registry;

import org.w3c.dom.Element;

public class RegistryActivity extends RegistryObject {
	
	public enum Type {
		award, course, event, program, project, dataset
	}
	
	public Type type;
	public String typeString;
	public String dateModified;
	
	public void setTypeString(final String type) {
		typeString = type;
		if (type.equals("PROJECT"))
			this.type = Type.project;
		else
			this.type = Type.valueOf(type);
	}
	
	public static RegistryActivity fromElement(Element element) {
		RegistryActivity registryObject = new RegistryActivity();
		
		registryObject.setTypeString(element.getAttribute("type"));
		registryObject.dateModified = element.getAttribute("dateModified");
		
		registryObject.initFromElement(element);

		return registryObject;
	}

}
