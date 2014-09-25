package org.grants.harvesters.pmh.registry;

import java.util.ArrayList;
import java.util.List;

import org.grants.harvesters.Harvester;
import org.w3c.dom.Element;

public class Phisical {
	public enum Type {
		unknown, streetAddress, postalAddress
	}
	
	public Type type;
	public String typeString;
	public List<AddressPart> addressParts;
	
	public void setTypeString(final String type) {
		this.typeString = type;
		if (null == type || type.length() == 0)
			this.type = Type.unknown;
		else
			this.type = Type.valueOf(type);
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
	
	public String GetAddress(AddressPart.Type type) {
		StringBuilder sb = new StringBuilder();
		for (AddressPart addressPart : addressParts) 
			if (addressPart.type == type && addressPart.IsValid()) {
				if (sb.length() > 0)
					sb.append(' ');
				sb.append(addressPart.value);
			}
		
		if (sb.length() > 0)
			return sb.toString();
		
		return null;
	}
	
	public String GetAddress() {
		StringBuilder sb = new StringBuilder();
		
		String address = GetAddress(AddressPart.Type.addressLine);
		if (null != address)
			sb.append(address);
		
		String text = GetAddress(AddressPart.Type.text);
		if (null != text)
		{
			if (sb.length() > 0)
				sb.append(", ");
			
			sb.append(text);			
		}
		
		String phone = GetAddress(AddressPart.Type.telephoneNumber);
		if (null != phone)
		{
			if (sb.length() > 0)
				sb.append(", ");
			sb.append("phone: ");
			sb.append(phone);			
		}
		
		String fax = GetAddress(AddressPart.Type.faxNumber);
		if (null != fax)
		{
			if (sb.length() > 0)
				sb.append(", ");
			sb.append("fax: ");
			sb.append(fax);			
		}
		
		String unknown = GetAddress(AddressPart.Type.unknwown);
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
