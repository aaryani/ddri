package org.grants.harvesters.pmh.registry;

import java.util.ArrayList;
import java.util.List;

import org.grants.harvesters.Harvester;
import org.w3c.dom.Element;

public class Address {

	public List<Phisical> phisicals;
	public List<Electronic> electronics;
	
	/*
	public void addPhisical(Phisical phisical) {
		if (null == phisicals)
			phisicals = new ArrayList<Phisical>();
		
		phisicals.add(phisical);
	}
	
	public void addElectronic(Electronic electronic) {
		if (null == electronics)
			electronics = new ArrayList<Electronic>();
		
		electronics.add(electronic);
	}	
	*/
	
	protected void setPhisical(List<Element> list) {
		if (null != list && list.size() > 0) {
			phisicals = new ArrayList<Phisical>();
			for (Element element : list)
				phisicals.add(Phisical.fromElement(element));
		}			
	}
	
	protected void setElectronic(List<Element> list) {
		if (null != list && list.size() > 0) {
			electronics = new ArrayList<Electronic>();
			for (Element element : list)
				electronics.add(Electronic.fromElement(element));
		}
	}	
	
	public static Address fromElement(Element element) {
		Address address = new Address();
		
		address.setPhisical(Harvester.getChildElementsByTagName(element, "phisical"));
		address.setElectronic(Harvester.getChildElementsByTagName(element, "electroinc"));
				
		return address;
	}
	
	public Phisical GetPhisical(Phisical.Type type) {
		if (null != phisicals)
			for (Phisical phisical : phisicals) 
				if (Phisical.Type.unknown == type || phisical.type == type) 
					return phisical;					
			
		return null;
	}
	
	public Electronic GetElectronic(Electronic.Type type) {
		if (null != electronics)
			for (Electronic electronic : electronics) 
				if (Electronic.Type.unknown == type || electronic.type == type) 
					return electronic;					
			
		return null;
	}
}
