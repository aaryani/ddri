package org.grants.crossref;

import org.codehaus.jackson.annotate.JsonProperty;

public class Query {
	private String searchTerms;
	private long startIndex;
	
	@JsonProperty("search-terms")
	public String getSearchTerms() {
		return searchTerms;		
	}
	
	@JsonProperty("start-index")
	public long getStartIndex() {
		return startIndex;
	}
	
	@JsonProperty("search-terms")
	public void setSearchTerms(final String searchTerms) {
		this.searchTerms = searchTerms;
	}
	
	@JsonProperty("start-index")
	public void setStartIndex(long startIndex) {
		this.startIndex = startIndex;
	}
	
	@Override
	public String toString() {
		return "Query [searchTerms=" + searchTerms + ", startIndex=" + startIndex + "]";
	}	
}
