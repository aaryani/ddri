package org.grants.harvesters.pmh.registry;

import java.util.ArrayList;
import java.util.List;

import org.grants.harvesters.Harvester;
import org.w3c.dom.Element;

public class RelatedInfo {
	public enum Type {
		unknown, 
	    publication, 				// any formally published document, whether available in digital or online form or not.
	    website, 					// any publicly accessible web location containing information related to the collection, activity, party or service.
	    reuseInformation, 			// information that supports reuse of data, such as data definitions, instrument calibration or settings, units of measurement, sample descriptions, experimental parameters, methodology, data analysis techniques, or data derivation rules.
	    dataQualityInformation, 	// data quality statements or summaries of data quality issues affecting the data.
	    metadata					// An alternative metadata format for the Object. This is most likely to be a discipline or system-specific format. E.g. NetCDF or ANZLIC.
	}
	
	public Type type;
	public String typeString;
	public String title;
	public String notes;
	public List<Identifier> identifiers;
	public List<Format> formats;
	public List<Relation> relations;
	
	public void setTypeString(final String type) {
		this.typeString = type;
		if (null == type || type.length() == 0)
			this.type = Type.unknown;
		else
			this.type = Type.valueOf(type);
	}
	
	/*
	protected void setTitle(NodeList list) {
		if (null != list && list.getLength() > 0)
			title = list.item(0).getTextContent();
	}
	
	protected void setNotes(NodeList list) {
		if (null != list && list.getLength() > 0)
			notes = list.item(0).getTextContent();
	}*/
	
	protected void setIdentifiers(List<Element> list) {
		if (null != list && list.size() > 0) {
			identifiers = new ArrayList<Identifier>();
			for (Element element : list)
				identifiers.add(Identifier.fromElement(element));
		}
	}	
	
	protected void setFormats(List<Element> list) {
		if (null != list && list.size() > 0) {
			formats = new ArrayList<Format>();
			for (Element element : list)
				formats.add(Format.fromElement(element));
		}
	}	
	
	protected void setRelations(List<Element> list) {
		if (null != list && list.size() > 0) {
			relations = new ArrayList<Relation>();
			for (Element element : list)
				relations.add(Relation.fromElement(element));
		}
	}	
	
	public static RelatedInfo fromElement(Element element) {
		RelatedInfo relatedInfo = new RelatedInfo();
		
		relatedInfo.setTypeString(element.getAttribute("type"));
		relatedInfo.title = Harvester.getChildElementTextByTagName(element, "title");
		relatedInfo.notes = Harvester.getChildElementTextByTagName(element, "notes");
		relatedInfo.setIdentifiers(Harvester.getChildElementsByTagName(element, "identifer"));
		relatedInfo.setFormats(Harvester.getChildElementsByTagName(element, "format"));
		relatedInfo.setRelations(Harvester.getChildElementsByTagName(element, "relation"));
		
		return relatedInfo;		
	}
}
