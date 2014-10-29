package org.grants.harvesters.grants;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Grant implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7269541738423606147L;
	private int nodeId;
	private String name;
	private String self;
	private Set<String> links = new HashSet<String>();
	
	public Grant() {		
	}	
	
	public Grant(final String name, final String link) {
		this.name = name;
		links.add(link);
	}
	
	public String getName() {
		return name;
	}
	
	@XmlElement
	public void setName(final String name) {
		this.name = name;
	}
	
	public Set<String> getLinks() {
		return links;
	}
	
	public void addLink(final String link) {
		links.add(link);
	}

	@XmlElement
	public void setLinks(Set<String> links) {
		this.links = links;
	}
	
	public int getNodeId() {
		return nodeId;
	}

	@XmlElement
	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}

	public String getSelf() {
		return self;
	}

	public void setSelf(String self) {
		this.self = self;
	}

	@Override
	public String toString() {
		return "Grant [nodeId=" + nodeId + ", name=" + name + ", self=" + self
				+ ", links=" + links + "]";
	}
}
