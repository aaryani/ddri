package org.grants.harvesters.pmh.registry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Record {
	public String identifier;
	public String datestamp;
	public Map<String, String> setSpecs;
	public List<RegistryObject> objects;
	
	public void addSpec(final String key, final String value) {
		if (null == setSpecs)
			setSpecs = new HashMap<String, String>();
		
		setSpecs.put(key, value);
	}
	
	public void addObject(RegistryObject object) {
		if (null == objects)
			objects = new ArrayList<RegistryObject>();
		
		objects.add(object);
	}
}
