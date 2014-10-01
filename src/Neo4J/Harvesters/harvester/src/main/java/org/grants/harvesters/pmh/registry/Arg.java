package org.grants.harvesters.pmh.registry;

import org.w3c.dom.Element;

public class Arg {
	public enum Type {
		unknown, string, object
	}
	
	public enum Use {
		unknown, inline, keyValue
	}

	public boolean required;
	public Type type;
	public String typeString;
	public Use use;
	public String useString;
	public String value;
		
	public void setTypeString(final String type) {
		this.typeString = type;
		try {
			this.type = Type.valueOf(type);
		} catch(Exception e) {
			System.out.println("Invalid Arg Type: " + type);
			this.type = Type.unknown;
		}
	}
	
	public void setUseString(final String use) {
		this.useString = use;
		try {
			this.use = Use.valueOf(use);
		} catch (Exception e) {
			System.out.println("Invalid Arg Use: " + type);
			this.use = Use.unknown;
		}
	}
	
	public static Arg fromElement(Element element) {
		Arg arg = new Arg();
		
		arg.required = element.getAttribute("required").equals("true");
		arg.setTypeString(element.getAttribute("type"));
		arg.setUseString(element.getAttribute("use"));
		arg.value = element.getTextContent();
		
		return arg;
	}

	public boolean isValid() {
		return null != value && !value.isEmpty();
	}
}
