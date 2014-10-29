package org.grants.harvesters.pmh;

import java.util.HashSet;
import java.util.Set;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Status {
	private Set<String> processedSets;
	private String currentSet;
	private String resumptionToken;
	
	public Set<String> getProcessedSets() {
		if (null == processedSets)
			processedSets = new HashSet<String>();
		return processedSets;
	}
	
	@XmlElement
	public void setProcessedSets(Set<String> processedSets) {
		this.processedSets = processedSets;
	}
	
	public void addProcessedSet(String processedSet) {
		getProcessedSets().add(processedSet);
	}

	public String getCurrentSet() {
		return currentSet;
	}

	@XmlElement
	public void setCurrentSet(String currentSet) {
		this.currentSet = currentSet;
	}

	public String getResumptionToken() {
		return resumptionToken;
	}

	@XmlElement
	public void setResumptionToken(String resumptionToken) {
		this.resumptionToken = resumptionToken;
	}

	@Override
	public String toString() {
		return "Status [processedSets=" + processedSets + ", currentSet="
				+ currentSet + ", resumptionToken=" + resumptionToken + "]";
	}
}
