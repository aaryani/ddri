package org.grants.harvesters.pmh.registry;

import java.util.ArrayList;
import java.util.List;

import org.grants.harvesters.Harvester;
import org.w3c.dom.Element;

public class RegistryCollection extends RegistryObject {

	public enum Type {
		unknown, catalogueOrIndex, collection, registry, 
		repository, dataset, catalogue, nonGeographicDataset,
		researchDataSet
	}
	
	public Type type;
	public String typeString;
	public String dateAccessioned;
	public String dateModified;
	
	/*
	 * Dates 
	 */	
	public List<Dates> dates;
	
	public void setTypeString(final String type) {
		this.typeString = type;
		try {
			if (null == type || type.length() == 0)
				this.type = Type.unknown;
			else if (type.equals("Data Collection"))
				this.type = Type.collection;
			else if (type.equals("Dataset"))
				this.type = Type.dataset;
			else if (type.equals("Catalogue"))
				this.type = Type.catalogue;
			else
				this.type = Type.valueOf(type);
		}
		catch(Exception e) {
			System.out.println("Invalid Registry collection type: " + type);
			
			this.type = Type.unknown;
		}
	}
		
	protected void setDates(List<Element> list) {
		if (null != list && list.size() > 0) {
			dates = new ArrayList<Dates>();
			for (Element element : list)
				dates.add(Dates.fromElement(element));
		}
	}	
		
	public static RegistryObject fromElement(Element element) {
		RegistryCollection registryObject = new RegistryCollection();
		
		registryObject.setTypeString(element.getAttribute("type"));
		registryObject.dateAccessioned = element.getAttribute("dateAccessioned");
		registryObject.dateModified = element.getAttribute("dateModified");
		registryObject.setDates(Harvester.getChildElementsByTagName(element, "dates"));
		registryObject.initFromElement(element);
		
		return registryObject;
	}
}
