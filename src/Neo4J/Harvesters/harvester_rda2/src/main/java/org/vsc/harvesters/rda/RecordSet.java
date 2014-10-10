package org.vsc.harvesters.rda;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RecordSet {
	public static final String FIELD_NUM_FOUND = "numFound";
	public static final String FIELD_START = "start";
	public static final String FIELD_DOCS = "docs";
	
	public Set<String> recordIds = new HashSet<String>();
	public Integer from;
	public Integer found;
	public Integer processed;
	
	public static RecordSet fromJson(Map<String, Object> json) throws Exception {
		// fist check that we have docs element in the response
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> docs = (List<Map<String, Object> >) json.get(FIELD_DOCS);
		if (null !=  docs) {
			// create RecorSet
			RecordSet recordSet = new RecordSet();
			recordSet.found = (Integer) json.get(FIELD_NUM_FOUND);
			recordSet.from = (Integer) json.get(FIELD_START);
			recordSet.processed = docs.size();
			
			for (Map<String, Object> doc : docs) {
				String id = (String) doc.get(Record.FIELD_ID);
				if (null != id && !id.isEmpty())
					recordSet.recordIds.add(id);
			}
			
			return recordSet;
		} else
			throw new Exception("Invalid response format, unbale to find obects array");
	}
}
