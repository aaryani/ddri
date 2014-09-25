package org.grants.harvesters.pmh.registry;

import java.util.ArrayList;
import java.util.List;

import org.grants.harvesters.Harvester;
import org.w3c.dom.Element;

public class RegistryService extends RegistryObject {

	public enum Type {
		unknown, create, generate, report, annotate, transform, assemble, 
	    harvest_oaipmh, search_http, search_opensearch, search_sru, 
	    search_srw, search_z3950, syndicate_atom, syndicate_rss, search_csw
	}
	
	public Type type;
	public String typeString;
	public String dateModified;
	
	public List<String> accessPolicies;
		
	public void setTypeString(final String type) {
		this.typeString = type;
		try {
			if (type.equals("search-CSW"))
				this.type = Type.search_csw;
			else
				this.type = Type.valueOf(type.replace('-', '_'));
		}
		catch(Exception e) {
			System.out.println("Invalid Registry Service type: " + type);
			
			this.type = Type.unknown;
		}
	}
	
	/*
	public void addAccessPolicy(String accessPolicy) {
		if (null == accessPolicies)
			accessPolicies = new ArrayList<String>();
		
		accessPolicies.add(accessPolicy);
	}*/
	
	protected void setAccessPolicy(List<Element> list) {
		if (null != list && list.size() > 0) {
			accessPolicies = new ArrayList<String>();
			for (Element element : list)
				accessPolicies.add(element.getTextContent());
		}
	}	
	
	public static RegistryService fromElement(Element element) {
		RegistryService registryObject = new RegistryService();
		
		registryObject.setTypeString(element.getAttribute("type"));
		registryObject.dateModified = element.getAttribute("dateModified");
		registryObject.setAccessPolicy(Harvester.getChildElementsByTagName(element ,"accessPolicy"));
		registryObject.initFromElement(element);
		
		return registryObject;
	}

}
