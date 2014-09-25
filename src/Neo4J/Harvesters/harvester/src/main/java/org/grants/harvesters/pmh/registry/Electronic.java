package org.grants.harvesters.pmh.registry;

import java.util.ArrayList;
import java.util.List;

import org.grants.harvesters.Harvester;
import org.w3c.dom.Element;

public class Electronic {
	public enum Type {
		unknown, 
	    email, 		// string used to receive messages by means of a computer network
	    other, 		// other electronic address
	    url, 		// Uniform Resource Locator
	    wsdl		// (service only) Web Service Definition Language
	}

	public Type type;
	public String typeString;
	public List<Arg> args;
	public String value;
	
	
	public void setTypeString(final String type) {
		this.typeString = type;
		if (null == type || type.length() == 0)
			this.type = Type.unknown;
		else
			this.type = Type.valueOf(type);
	}

	/*
	public void addArg(Arg arg) {
		if (null == args)
			args = new ArrayList<Arg>();
		
		args.add(arg);
	}*/
		
	protected void setArgs(List<Element> list) {
		if (null != list && list.size() > 0) {
			args = new ArrayList<Arg>();
			for (Element element : list)
				args.add(Arg.fromElement(element));
		}
	}	
	
	public static Electronic fromElement(Element element) {
		Electronic electronic = new Electronic();
		
		electronic.setTypeString(element.getAttribute("type"));
		electronic.setArgs(Harvester.getChildElementsByTagName(element, "arg"));
		electronic.value = Harvester.getChildElementTextByTagName(element, "value");
		
		return electronic;
	}
	
	public String GetAddress() {
		if (null != value && !value.isEmpty()) {
			String address = value;
			
			if (null != args && !args.isEmpty()) {
				StringBuilder sb = new StringBuilder();
				for (Arg arg : args) {
					if (sb.length() > 0)
						sb.append(", ");
					sb.append(arg.value);
					sb.append(':');
					sb.append(arg.typeString);
					sb.append(' ');
					sb.append(arg.useString);					
				}
				
				if (sb.length() > 0)
					address += "(" + sb.toString() + ")";				
			}

			return address;
		}
		
		return null;
	}

}
