package org.isbar_software.neo4j.schema.constrain;

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
import org.isbar_software.neo4j.schema.index.Index;
import org.isbar_software.neo4j.utils.Neo4JException;
import org.isbar_software.neo4j.utils.Neo4JInterface;
import org.isbar_software.neo4j.utils.Neo4JOperation;
import org.isbar_software.neo4j.utils.OperationTypes;

import com.sun.jersey.api.client.ClientResponse;

public class ConstraintInterface extends Neo4JInterface {
	private static final TypeReference<ArrayList<Constraint>> arrayListIndexTypeReference = new TypeReference<ArrayList<Constraint>>() {};   
	
    private Map<String, List<Constraint>> constraintCashe = null;
	
	public ConstraintInterface(Database db) {
		super(db);
	}

	@Override
	public String getUri() {
		return db.getServerUri() + "schema/constraint";
	}
	
	public String getLabelUri(final String label) {
		return getUri() + "/" + label + "/uniqueness/";
	}
	
	
	// Index cashe
		  
    private Map<String, List<Constraint>> getConstraintCashe() {
    	if (null == constraintCashe)
    		constraintCashe = new HashMap<String, List<Constraint>>();
    	
    	return constraintCashe;
    }    
    
    private Neo4JOperation listConstraintsForLabelOperation(String label) {
		return new Neo4JOperation(OperationTypes.GET, 
				getLabelUri(label));
	}
	
	public List<Constraint> listConstraintsForLabel(final String label) 
    		throws JsonParseException, JsonMappingException, IOException 
    {
    	Map<String, List<Constraint>> constraintCashe = getConstraintCashe();
    	if (constraintCashe.containsKey(label))
    		return constraintCashe.get(label);
    	
    	ClientResponse response = Invoke(listConstraintsForLabelOperation(label));
    	
    	int status = response.getStatus();
    	if (status == 200)
    	{
    		final String entitity = response.getEntity( String.class );
    		response.close();
    	
    		List<Constraint> constraints = mapper.readValue(entitity, arrayListIndexTypeReference);
    		if (constraints != null)
    		{
    			// set db link and update index uri
    			for (Constraint constraint : constraints) 
    				constraint.setDatabase(db);
    			constraintCashe.put(label, constraints);
    		}
    		return constraints;
    	}
    	else
    	{
    		response.close();
    		return null;
    	}
    } 
	
	public Constraint findConstraint(final String label, final String ... keys) 
    		throws JsonParseException, JsonMappingException, IOException
    {
    	List<Constraint> constraints = listConstraintsForLabel(label);
    	if (constraints != null)
    		for (Constraint constraint : constraints)
    			if (constraint.getLabel().compareTo(label) == 0 &&
    			    Arrays.equals(constraint.getPropertyKeys(), keys))
    				return constraint;
    	
    	return null;    	
    }
    
	 private Neo4JOperation createConstraintOperation(String label, final String ... keys) {
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
    			 getLabelUri(label),
    			 sb.toString());
	}		
	
    public Constraint createConstraint(final String label, final String ... keys) 
    		throws JsonParseException, JsonMappingException, IOException, Neo4JException
    {
    	// do not create index, if it already exists
    	Constraint constraint = findConstraint(label, keys);
    	if (null != constraint)
    		return constraint;
    	
    	ClientResponse response = Invoke(createConstraintOperation(label, keys));
    	
    	final String entitity = response.getEntity( String.class );
    	int status = response.getStatus();  
    	response.close();
    	
    	if (status == 200)
    	{
    		constraint = mapper.readValue(entitity, Constraint.class);
    		constraint.setDatabase(db);

    		Map<String, List<Constraint>> constraintCashe = getConstraintCashe();
    		if (constraintCashe.containsKey(label))
    			constraintCashe.get(label).add(constraint);
    		else
    		{
    			List<Constraint> constraints = new ArrayList<Constraint>();
    			constraints.add( constraint );    			
    			constraintCashe.put( label, constraints );
    		}
    		
    		return constraint;
    	}
    	else
    		throw mapper.readValue(entitity, Neo4JException.class);    	
    }
}
