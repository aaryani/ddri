package org.grants.harvesters.pmh.registry;

import org.w3c.dom.Element;

public class Arg {
	public enum Type {
		string, object
	}
	
	public enum Use {
		inline, keyValue
	}

	public boolean required;
	public Type type;
	public String typeString;
	public Use use;
	public String useString;
	public String value;
	
	public void setTypeString(final String type) {
		this.typeString = type;
		this.type = Type.valueOf(type);
	}
	
	public void setUseString(final String use) {
		this.useString = use;
		this.use = Use.valueOf(use);
	}
	
	public static Arg fromElement(Element element) {
		Arg arg = new Arg();
		
		arg.required = element.getAttribute("required").equals("true");
		arg.setTypeString(element.getAttribute("type"));
		arg.setUseString(element.getAttribute("use"));
		arg.value = element.getTextContent();
		
		return arg;
	}

	public boolean IsValid() {
		return null != value && !value.isEmpty();
	}
}
