package org.grants.harvesters.pmh.registry;

import java.util.ArrayList;
import java.util.List;

import org.grants.harvesters.Harvester;
import org.w3c.dom.Element;

public class Phisical {
	protected static final String TYPE_STREET_ADDRESS = "streetAddress";
	protected static final String TYPE_POSTAL_ADDRESS = "postalAddress";
	
	public enum Type {
		unknown, street, postal
	}
	
	public Type type;
	public String typeString;
	public List<AddressPart> addressParts;
	
	public void setTypeString(final String type) {
		this.typeString = type;
		if (null == type || type.isEmpty())
			this.type = Type.unknown;
		else if (type.equals(TYPE_STREET_ADDRESS))
			this.type = Type.street;
		else if (type.equals(TYPE_POSTAL_ADDRESS))
			this.type = Type.postal;
		else {
			System.out.println("Invalid Phisical Type: " + type);
			this.type = Type.unknown;
		}
	}
		
	protected void setAddressParts(List<Element> list) {
		if (null != list && list.size() > 0) {
			addressParts = new ArrayList<AddressPart>();
			for (Element element : list)
				addressParts.add(AddressPart.fromElement(element));
		}
	}	
	
	public static Phisical fromElement(Element element) {
		Phisical phisical = new Phisical();
		
		phisical.setTypeString(element.getAttribute("type"));
		phisical.setAddressParts(Harvester.getChildElementsByTagName(element, "addressPart"));
		
		return phisical;
	}
	
	public String toString(AddressPart.Type type) {
		StringBuilder sb = new StringBuilder();
		for (AddressPart addressPart : addressParts) 
			if (addressPart.type == type && addressPart.isValid()) {
				if (sb.length() > 0)
					sb.append(' ');
				sb.append(addressPart.value);
			}
		
		if (sb.length() > 0)
			return sb.toString();
		
		return null;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		String address = toString(AddressPart.Type.addressLine);
		if (null != address)
			sb.append(address);
		
		String text = toString(AddressPart.Type.text);
		if (null != text)
		{
			if (sb.length() > 0)
				sb.append(", ");
			
			sb.append(text);			
		}
		
		String phone = toString(AddressPart.Type.telephoneNumber);
		if (null != phone)
		{
			if (sb.length() > 0)
				sb.append(", ");
			sb.append("phone: ");
			sb.append(phone);			
		}
		
		String fax = toString(AddressPart.Type.faxNumber);
		if (null != fax)
		{
			if (sb.length() > 0)
				sb.append(", ");
			sb.append("fax: ");
			sb.append(fax);			
		}
		
		String unknown = toString(AddressPart.Type.unknwown);
		if (null != unknown)
		{
			if (sb.length() > 0)
				sb.append(", ");
			sb.append(unknown);			
		}

		if (sb.length() > 0)
			return sb.toString();
		
		return null;
	}
}
