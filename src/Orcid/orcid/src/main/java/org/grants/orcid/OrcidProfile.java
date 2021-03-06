package org.grants.orcid;

import org.codehaus.jackson.annotate.JsonProperty;

public class OrcidProfile {
	private String orcid;
	private OrcidIdentifier identifier;
	private OrcidPreferences preferences;
	private OrcidHistory history;
	private OrcidBio bio;
	private OrcidActivities activities;
	private String type;
	private String groupType;
	private String clientType;

	public String getOrcid() {
		return orcid;
	}

	public void setOrcid(String orcid) {
		this.orcid = orcid;
	}

	@JsonProperty("orcid-identifier")
	public OrcidIdentifier getIdentifier() {
		return identifier;
	}

	@JsonProperty("orcid-identifier")
	public void setIdentifier(OrcidIdentifier identifier) {
		this.identifier = identifier;
	}

	@JsonProperty("orcid-preferences")
	public OrcidPreferences getPreferences() {
		return preferences;
	}

	@JsonProperty("orcid-preferences")
	public void setPreferences(OrcidPreferences preferences) {
		this.preferences = preferences;
	}

	@JsonProperty("orcid-history")
	public OrcidHistory getHistory() {
		return history;
	}

	@JsonProperty("orcid-history")
	public void setHistory(OrcidHistory history) {
		this.history = history;
	}

	@JsonProperty("orcid-bio")
	public OrcidBio getBio() {
		return bio;
	}

	@JsonProperty("orcid-bio")
	public void setBio(OrcidBio bio) {
		this.bio = bio;
	}

	@JsonProperty("orcid-activities")
	public OrcidActivities getActivities() {
		return activities;
	}

	@JsonProperty("orcid-activities")
	public void setActivities(OrcidActivities activities) {
		this.activities = activities;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	
	@JsonProperty("group-type")
	public String getGroupType() {
		return groupType;
	}

	@JsonProperty("group-type")
	public void setGroupType(String groupType) {
		this.groupType = groupType;
	}

	@JsonProperty("client-type")
	public String getClientType() {
		return clientType;
	}

	@JsonProperty("client-type")
	public void setClientType(String clientType) {
		this.clientType = clientType;
	}

	@Override
	public String toString() {
		return "OrcidProfile [orcid=" + orcid + ", identifier=" + identifier
				+ ", preferences=" + preferences + ", history=" + history
				+ ", bio=" + bio + ", activities=" + activities + ", type="
				+ type + ", groupType=" + groupType + ", clientType="
				+ clientType + "]";
	}
}
