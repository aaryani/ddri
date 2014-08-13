package org.isbar_software.neo4j.schema.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.type.TypeReference;
import org.isbar_software.neo4j.Database;
import org.isbar_software.neo4j.utils.Neo4JException;
import org.isbar_software.neo4j.utils.Neo4JInterface;
import org.isbar_software.neo4j.utils.Neo4JOperation;
import org.isbar_software.neo4j.utils.OperationTypes;

import com.sun.jersey.api.client.ClientResponse;

public class IndexInterface extends Neo4JInterface {
	private static final TypeReference<ArrayList<Index>> arrayListIndexTypeReference = new TypeReference<ArrayList<Index>>() {};   
	
    private Map<String, List<Index>> indexCashe = null;
	
	public IndexInterface(Database db) {
		super(db);
	}

	@Override
	public String getUri() {
		return db.getServerUri() + "schema/index";
	}
	
	// Index cashe
		  
    private Map<String, List<Index>> getIndexCashe() {
    	if (null == indexCashe)
    		indexCashe = new HashMap<String, List<Index>>();
    	
    	return indexCashe;
    }    
    
    private Neo4JOperation listIndexesForLabelOperation(String label) {
		return new Neo4JOperation(OperationTypes.GET, 
					getUri() + "/" + label);
	}
	
	public List<Index> listIndexesForLabel(final String label) 
    		throws JsonParseException, JsonMappingException, IOException 
    {
    	Map<String, List<Index>> indexCashe = getIndexCashe();
    	if (indexCashe.containsKey(label))
    		return indexCashe.get(label);
    	
    	ClientResponse response = Invoke(listIndexesForLabelOperation(label));
    	
    	int status = response.getStatus();
    	if (status == 200)
    	{
    		final String entitity = response.getEntity( String.class );
    		response.close();
    	
    		List<Index> indexes = mapper.readValue(entitity, arrayListIndexTypeReference);
    		if (indexes != null)
    		{
    			// set db link and update index uri
    			for (Index index : indexes) 
    				index.setDatabase(db);
    			indexCashe.put(label, indexes);
    		}
    		return indexes;
    	}
    	else
    	{
    		response.close();
    		return null;
    	}
    } 
	
	public Index findIndex(final String label, final String ... keys) 
    		throws JsonParseException, JsonMappingException, IOException
    {
    	List<Index> indexes = listIndexesForLabel(label);
    	if (indexes != null)
    		for (Index index : indexes)
    			if (index.getLabel().compareTo(label) == 0 &&
    			    Arrays.equals(index.getPropertyKeys(), keys))
    				return index;
    	
    	return null;    	
    }
    
	 private Neo4JOperation createIndexOperation(String label, final String ... keys) {
		 StringBuilder sb = new StringBuilder();
    	 sb.append(" {\"property_keys\":[");
    	 for (int i = 0; i < keys.length; ++i)
    	 {
    		 if (i > 0)
    			 sb.append(",");
    		 sb.append('"' + keys[i] + '"');
    	 }
    	 sb.append("]}");
    	
    	 return new Neo4JOperation(OperationTypes.POST,
    			 getUri() + "/" + label,
    			 sb.toString());
	}		
	
    public Index createIndex(final String label, final String ... keys) 
    		throws JsonParseException, JsonMappingException, IOException, Neo4JException
    {
    	// do not create index, if it already exists
    	Index index = findIndex(label, keys);
    	if (null != index)
    		return index;
    	
    	ClientResponse response = Invoke(createIndexOperation(label, keys));
    	
    	final String entitity = response.getEntity( String.class );
    	int status = response.getStatus();  
    	response.close();
    	
    	if (status == 200)
    	{
    		index = mapper.readValue(entitity, Index.class);
    		index.setDatabase(db);

    		Map<String, List<Index>> indexCashe = getIndexCashe();
    		if (indexCashe.containsKey(label))
    			indexCashe.get(label).add(index);
    		else
    		{
    			List<Index> indexes = new ArrayList<Index>();
    			indexes.add( index );    			
    			indexCashe.put( label, indexes );
    		}
    		
    		return index;
    	}
    	else
    		throw mapper.readValue(entitity, Neo4JException.class);    	
    }
    
}
