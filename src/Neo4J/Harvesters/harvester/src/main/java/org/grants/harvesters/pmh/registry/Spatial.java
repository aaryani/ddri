package org.grants.harvesters.pmh.registry;

import org.w3c.dom.Element;

public class Spatial {
	public enum Type {
	    gmlKmlPolyCoords, 	// A set of KML long/lat co-ordinates derived from GML (OpenGIS Geography Markup Language) defining a polygon as described by the KML coordinates element but without the altitude component
	    gpx, 				// the GPS Exchange Format
	    iso31661, 			// ISO 3166-1 Codes for the representation of names of countries and their subdivisions - Part 1: Country codes
	    iso31662, 			// Codes for the representation of names of countries and their subdivisions - Part 2: Country subdivision codes
	    iso31663, 			// ISO 3166-3 Codes for country names which have been deleted from ISO 3166-1 since its first publication in 1974.
	    iso19139dcmiBox, 	// DCMI Box notation derived from bounding box metadata conformant with the iso19139 schema
	    kmlPolyCoords, 		// A set of KML (Keyhole Markup Language) long/lat co-ordinates defining a polygon as described by the KML coordinates element
	    dcmiPoint, 			// spatial location information specified in DCMI Point notation
	    text,				// free-text representation of spatial location
	    gml
	}
	
	public Type type;
	public String typeString;
	public String value;
	
	public void setTypeString(final String type) {
		this.typeString = type;
		this.type = Type.valueOf(type);
	}
		
	public static Spatial fromElement(Element element) {
		Spatial spatial = new Spatial();
		
		spatial.setTypeString(element.getAttribute("type"));
		spatial.value = element.getTextContent();
		
		return spatial;
	}	
}
