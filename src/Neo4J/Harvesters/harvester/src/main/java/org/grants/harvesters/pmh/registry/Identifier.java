package org.grants.harvesters.pmh.registry;

import org.w3c.dom.Element;

public class Identifier {
	protected static final String AU_ANL_PEAU = "AU-ANL:PEAU";
	protected static final String AU_QU_LOCAL = "AU-QU-local";
	protected static final String DEEDI_AUTHOR = "DEEDI-Author";
	
	public enum Type {
	    unknown,
		abn,			// Australian Business Number
	    arc, 			// Australian Research Council identifier
	    ark, 			// ARK Persistent Identifier Scheme
	    doi, 			// Digital Object Identifier
	    handle, 		// HANDLE System Identifier
	    infouri, 		// 'info' URI scheme
	    isil, 			// International Standard Identifier for Libraries
	    local, 			// identifer unique within a local context
	    au_anl_peau,	// AU-ANL:PEAU: National Library of Australia identifier
	    purl, 			// Persistent Uniform Resource Locator
	    uri, 			// Uniform Resource Identifier
	    orcid,			// ORCID Identifier
	    nhmrc,
	    au_qu_local,
	    deedi_author
	}
	
	public Type type;
	
	public String typeString;
	public String value;
	
	protected void setTypeString(String type) {
		typeString = type;
		
		try {
			if (type.equals(AU_ANL_PEAU))
				this.type = Type.au_anl_peau;
			else if (type.equals(AU_QU_LOCAL)) 
				this.type = Type.au_qu_local;
			else if (type.equals(DEEDI_AUTHOR))
				this.type = Type.deedi_author;
			else
				this.type = Type.valueOf(type);
		}
		catch(Exception e) {
			System.out.println("Invalid Idenitifier type: " + type);
			
			this.type = Type.unknown;
		}		
	}
	
	public static Identifier fromElement(Element element) {
		Identifier identifier = new Identifier();
		
		identifier.setTypeString(element.getAttribute("type"));
		identifier.value = element.getTextContent();
		
		return identifier;
	}
	
	public boolean isValid() {
		return null != value && !value.isEmpty(); 
	}
	
	public String toString() {
		return value;
	}
}
