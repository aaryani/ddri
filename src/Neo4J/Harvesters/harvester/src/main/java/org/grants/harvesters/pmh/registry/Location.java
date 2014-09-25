package org.grants.harvesters.pmh.registry;

import java.util.ArrayList;
import java.util.List;

import org.grants.harvesters.Harvester;
import org.w3c.dom.Element;

public class Location {

//	public String type; // not in use
	public String dateFrom;
	public String dateTo;
	
	public List<Address> addresses;
	public List<Spatial> spatials;
	 
	/*
	public void addAddress(Address address) {
		if (null == addresses)
			addresses = new ArrayList<Address>();
		
		addresses.add(address);		
	}
	
	public void addSpatial(Spatial spatial) {
		if (null == spatials)
			spatials = new ArrayList<Spatial>();
		
		spatials.add(spatial);		
	}*/
	
	protected void setAddresses(List<Element> list) {
		if (null != list && list.size() > 0) {
			addresses = new ArrayList<Address>();
			for (Element element : list)
				addresses.add(Address.fromElement(element));
		}			
	}
	
	protected void setSpatials(List<Element> list) {
		if (null != list && list.size() > 0) {
			spatials = new ArrayList<Spatial>();
			for (Element element : list)
				spatials.add(Spatial.fromElement(element));
		}			
	}
	
	public static Location fromElement(Element element) {
		Location location = new Location();
		
		location.dateFrom = element.getAttribute("dateFrom");
		location.dateTo = element.getAttribute("dateTo");
		location.setAddresses(Harvester.getChildElementsByTagName(element, "address"));
		location.setSpatials(Harvester.getChildElementsByTagName(element, "spatial"));
		
		return location;
	}
}
