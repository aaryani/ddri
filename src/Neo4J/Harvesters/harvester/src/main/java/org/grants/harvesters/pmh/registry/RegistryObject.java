package org.grants.harvesters.pmh.registry;

import java.util.ArrayList;
import java.util.List;

import org.grants.harvesters.Harvester;
import org.w3c.dom.Element;

public class RegistryObject {
	
	/*
	 * Object Key 
	 */
	public String key;

	/*
	 * Originating Source
	 */
	public OriginatingSource originatingSource;
	
	/*
	 * Identifiers
	 */
	public List<Identifier> identifiers;
	
	/*
	 * Names
	 */
	public List<Name> names;
	
	/*
	 * Location
	 */
	public List<Location> locations;

	/*
	 * Subjects
	 */
	public List<Subject> subjects;
	
	/*
	 * Descriptions
	 */
	public List<Description> descriptions;
	
	/*
	 * Related Object
	 */
	public List<RelatedObject> relatedObjects;
	
	/*
	 * Related Info
	 */
	public List<RelatedInfo> relatedInfos;

	protected void setIdentifiers(List<Element> list) {
		if (null != list && list.size() > 0) {
			identifiers = new ArrayList<Identifier>();
			for (Element element : list)
				identifiers.add(Identifier.fromElement(element));
		}
	}	
	
	protected void setNames(List<Element> list) {
		if (null != list && list.size() > 0) {
			names = new ArrayList<Name>();
			for (Element element : list)
				names.add(Name.fromElement(element));
		}
	}	
	
	protected void setLocations(List<Element> list) {
		if (null != list && list.size() > 0) {
			locations = new ArrayList<Location>();
			for (Element element : list)
				locations.add(Location.fromElement(element));
		}
	}	
	
	protected void setSubjects(List<Element> list) {
		if (null != list && list.size() > 0) {
			subjects = new ArrayList<Subject>();
			for (Element element : list)
				subjects.add(Subject.fromElement(element));
		}
	}	
		
	protected void setDescriptions(List<Element> list) {
		if (null != list && list.size() > 0) {
			descriptions = new ArrayList<Description>();
			for (Element element : list)
				descriptions.add(Description.fromElement(element));
		}
	}	
		
	protected void setRelatedObjects(List<Element> list) {
		if (null != list && list.size() > 0) {
			relatedObjects = new ArrayList<RelatedObject>();
			for (Element element : list)
				relatedObjects.add(RelatedObject.fromElement(element));
		}
	}	
	
	protected void setRelatedInfos(List<Element> list) {
		if (null != list && list.size() > 0) {
			relatedInfos = new ArrayList<RelatedInfo>();
			for (Element element : list)
				relatedInfos.add(RelatedInfo.fromElement(element));
		}
	}	
	
	public void initFromElement(Element element) {
		setIdentifiers(Harvester.getChildElementsByTagName(element, "identifier"));
		setNames(Harvester.getChildElementsByTagName(element, "name"));
		setLocations(Harvester.getChildElementsByTagName(element, "location"));
		setSubjects(Harvester.getChildElementsByTagName(element, "subject"));
		setDescriptions(Harvester.getChildElementsByTagName(element, "description"));
		setRelatedObjects(Harvester.getChildElementsByTagName(element, "description"));
	}
	
	public Name getPrimaryName() {
		if (null != names)
			for (Name name : names) 
				if (name.type == Name.Type.primary)
					return name;
		
		return null;		
	}
	
	public Name getAnyName() {
		if (null != names && names.size() > 0)
			return names.get(0);
		
		return null;		
	}
	
	public Phisical GetPhisicalAddress(Phisical.Type type) {
		Phisical phisical;
		if (null != locations) 
			for (Location location : locations) 
				if (null != location.addresses)
					for (Address address : location.addresses) 
						if ((phisical = address.GetPhisical(type)) != null)
							return phisical;
							
		return null;
	}
	
	public Electronic GetElectronicAddress(Electronic.Type type) {
		Electronic electronic;
		if (null != locations) 
			for (Location location : locations) 
				if (null != location.addresses)
					for (Address address : location.addresses)
						if ((electronic = address.GetElectronic(type)) != null)
							return electronic;
							
		return null;
	}
}

/*
<activity> Element

Wrapper element for descriptive and administrative metadata for an activity registry object.

May contain: description (registryObject) | identifier (registryObject) | location | name | relatedInfo | relatedObject | subject
*/

/*
<collection> Element

Wrapper element for descriptive and administrative metadata for collection registry object.

May contain: description (registryObject) | identifier (registryObject) | location | name | relatedInfo | relatedObject | subject | dates
*/

/*
<party> Element

Wrapper element for descriptive and administrative metadata for a party registry object.

May contain: description (registryObject) | identifier (registryObject) | location | name | relatedInfo | relatedObject | subject
*/

/*
<service> Element

Wrapper element for descriptive and administrative metadata for service registry object.

May contain: accessPolicy | description | identifier | location | name | relatedInfo | relatedObject | subject
*/

