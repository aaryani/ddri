import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

import au.com.bytecode.opencsv.CSVReader;

/**
 * 
 */

/**
 * @author dima
 *
 */
public class Loader {

	private static final String DB_PATH = "/home/dima/Projects/Grants/ddri/db/grants.db";
	private static final String GRANTS_CSV_PATH = "/home/dima/Projects/Grants/ddri/data/nhmrc/2014/grants-data.csv";
	private static final String ROLES_CSV_PATH = "/home/dima/Projects/Grants/ddri/data/nhmrc/2014/ci-roles.csv";
	
	private static final String LABEL_GRANT = "Grant";
	private static final String LABEL_GRANTEE = "Grantee";
	
	private static final String FIELD_GRANT_ID = "grant_id";
	private static final String FIELD_APPLICATION_YEAR = "application_year";
	private static final String FIELD_SUB_TYPE = "sub_type";
	private static final String FIELD_HIGHER_GRANT_TYPE = "higher_grant_type";
	private static final String FIELD_SCIENTIFIC_TITLE = "scientific_title";
	private static final String FIELD_SIMPLIFIED_TITLE = "simplified_title";
	private static final String FIELD_CIA_NAME = "cia_name";
	private static final String FIELD_START_YEAR = "start_year";
	private static final String FIELD_END_YEAR = "end_year";
	private static final String FIELD_TOTAL_BUDGET = "total_budget";
	private static final String FIELD_RESEARCH_AREA = "research_area";
	private static final String FIELD_FOR_CATEGORY = "for_category";
	private static final String FIELD_OF_RESEARCH = "field_of_research";
	private static final String FIELD_ADMIN_INSTITUTION = "admin_institution";
	private static final String FIELD_ADMIN_INSTITUTION_STATE = "admin_institution_state";
	private static final String FIELD_ADMIN_INSTITUTION_TYPE = "admin_institution_type";
	private static final String FIELD_KEYWORDS = "keywords";
	private static final String FIELD_HEALTH_KEYWORDS = "health_keywords";
	private static final String FIELD_MEDIA_SUMMARY = "media_summary";
	private static final String FIELD_SOURCE_SYSTEM = "source_system";
	
	private static final String FIELD_ROLE = "role";
	private static final String FIELD_DW_INDIVIDUAL_ID = "dw_individual_id";
	private static final String FIELD_SOURCE_INDIVIDUAL_ID = "source_individual_id";
	private static final String FIELD_TITLE = "title";
	private static final String FIELD_FIRST_NAME = "first_name";
	private static final String FIELD_MIDDLE_NAME = "middle_name";
	private static final String FIELD_LAST_NAME = "last_name";
	private static final String FIELD_FULL_NAME = "full_name";
	private static final String FIELD_ROLE_START_DATE = "role_start_date";
	private static final String FIELD_ROLE_END_DATE = "role_end_date";
	
	private static enum RelTypes implements RelationshipType
	{
	    ROLE
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// init db
		GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
		registerShutdownHook( graphDb );
		
		// create index on grant_id if not exist
		try ( Transaction tx = graphDb.beginTx() )
		{
			Label labelGrant = DynamicLabel.label( LABEL_GRANT );
						
		    Schema schema = graphDb.schema();
		    
		    Iterable<IndexDefinition> x = schema.getIndexes(labelGrant);
		    if (x == null)
		    {
		    	schema.indexFor( DynamicLabel.label( LABEL_GRANT ) )
            		.on( FIELD_GRANT_ID )
            		.create();
		    }
		    
		    tx.success();
		}
		
		// load grant csv
		try ( Transaction tx = graphDb.beginTx() )
		{
			Label labelGrant = DynamicLabel.label( LABEL_GRANT );
			
			CSVReader reader = new CSVReader(new FileReader(GRANTS_CSV_PATH));
			String[] grant;
			boolean header = false;
			while ((grant = reader.readNext()) != null) 
			{
				if (!header)
				{
					header = true;
					continue;
				}
				if (grant.length != 57)
					continue;
				
				int grantId = Integer.parseInt(grant[0]);
				int applicationYear = Integer.parseInt(grant[1]);
				String subType = grant[2];
				String higherGrantType = grant[3];
			//	String mainFundingGroup = grant[4];
		//		String didFunded = grant[5];
				String adminInstitution = grant[6];
				String adminInstitutionState = grant[7];
				String adminInstitutionType = grant[8];
				String scientificTitle = grant[9];
				String simplifiedTitle = grant[10];
				String ciaName = grant[11];
				int startYear = Integer.parseInt(grant[12]);
				int endYear = Integer.parseInt(grant[13]);
				String totalBudget = grant[41];
				String researchArea = grant[42];
				String forCategory = grant[43];
				String fieldOfResearch = grant[44];
				String keyword1 = grant[45];
				String keyword2 = grant[46];
				String keyword3 = grant[47];
				String keyword4 = grant[48];
				String keyword5 = grant[49];
				String healthKeyword1 = grant[50];
				String healthKeyword2 = grant[51];
				String healthKeyword3 = grant[52];
				String healthKeyword4 = grant[53];
				String healthKeyword5 = grant[54];
				String mediaSummary = grant[54];
				String sourceSystem = grant[56];
				
				Node nodeGrant = graphDb.createNode( labelGrant );
				nodeGrant.setProperty(FIELD_GRANT_ID, grantId);
				nodeGrant.setProperty(FIELD_APPLICATION_YEAR, applicationYear);
				nodeGrant.setProperty(FIELD_SUB_TYPE, subType);
				nodeGrant.setProperty(FIELD_HIGHER_GRANT_TYPE, higherGrantType);
				nodeGrant.setProperty(FIELD_ADMIN_INSTITUTION, adminInstitution);
				nodeGrant.setProperty(FIELD_ADMIN_INSTITUTION_STATE, adminInstitutionState);
				nodeGrant.setProperty(FIELD_ADMIN_INSTITUTION_TYPE, adminInstitutionType);
				nodeGrant.setProperty(FIELD_SCIENTIFIC_TITLE, scientificTitle);
				nodeGrant.setProperty(FIELD_SIMPLIFIED_TITLE, simplifiedTitle);
				nodeGrant.setProperty(FIELD_CIA_NAME, ciaName);
				nodeGrant.setProperty(FIELD_START_YEAR, startYear);
				nodeGrant.setProperty(FIELD_END_YEAR, endYear);
				nodeGrant.setProperty(FIELD_TOTAL_BUDGET, totalBudget);
				nodeGrant.setProperty(FIELD_RESEARCH_AREA, researchArea);
				nodeGrant.setProperty(FIELD_FOR_CATEGORY, forCategory);
				nodeGrant.setProperty(FIELD_OF_RESEARCH, fieldOfResearch);
				nodeGrant.setProperty(FIELD_KEYWORDS, new String[] { keyword1, keyword2, keyword3, keyword4, keyword5 });
				nodeGrant.setProperty(FIELD_HEALTH_KEYWORDS, new String[] { healthKeyword1, healthKeyword2, healthKeyword3, healthKeyword4, healthKeyword5 });
				nodeGrant.setProperty(FIELD_MEDIA_SUMMARY, mediaSummary);
				nodeGrant.setProperty(FIELD_SOURCE_SYSTEM, sourceSystem);
				
			}

			reader.close();
			
			tx.success();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// load researchers data
		try ( Transaction tx = graphDb.beginTx() )
		{
			Label labelGrant = DynamicLabel.label( LABEL_GRANT );
			Label labelGrantee = DynamicLabel.label( LABEL_GRANTEE );
			
			CSVReader reader = new CSVReader(new FileReader(ROLES_CSV_PATH));
			String[] grantee;
			boolean header = false;
			while ((grantee = reader.readNext()) != null) 
			{
				if (!header)
				{
					header = true;
					continue;
				}
				if (grantee.length != 12)
					continue;

				int grantId = Integer.parseInt(grantee[0]);
				String role = grantee[1];
				String dwIndividualId = grantee[2];
				String sourceIndividualId = grantee[3];
				String title = grantee[4];
				String firstName = grantee[5];
				String middleName = grantee[6];
				String lastName = grantee[7];
				String fullName = grantee[8];
				String roleStartDate = grantee[9];
				String roleEndDate = grantee[10];
				String sourceSystem = grantee[11];
				
				Node nodeGrantee = graphDb.createNode( labelGrantee );
				nodeGrantee.setProperty(FIELD_ROLE, role);
				nodeGrantee.setProperty(FIELD_DW_INDIVIDUAL_ID, dwIndividualId);
				nodeGrantee.setProperty(FIELD_SOURCE_INDIVIDUAL_ID, sourceIndividualId);
				nodeGrantee.setProperty(FIELD_TITLE, title);
				nodeGrantee.setProperty(FIELD_FIRST_NAME, firstName);
				nodeGrantee.setProperty(FIELD_MIDDLE_NAME, middleName);
				nodeGrantee.setProperty(FIELD_LAST_NAME, lastName);
				nodeGrantee.setProperty(FIELD_FULL_NAME, fullName);
				nodeGrantee.setProperty(FIELD_ROLE_START_DATE, roleStartDate);
				nodeGrantee.setProperty(FIELD_ROLE_END_DATE, roleEndDate);
				nodeGrantee.setProperty(FIELD_SOURCE_SYSTEM, sourceSystem);
				
				// create relations
				try ( ResourceIterator<Node> grants =
			            graphDb.findNodesByLabelAndProperty( labelGrant, FIELD_GRANT_ID, grantId ).iterator() )
			    {
			        while ( grants.hasNext() )
			        {
			        	Node grant = grants.next();
			        	
			        	nodeGrantee.createRelationshipTo(grant, RelTypes.ROLE );
			        }			        
			    }
			}
			
			reader.close();
			
			tx.success();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		// shutdown db
		graphDb.shutdown();
		
		System.out.print( "done!" );

	}
	
	private static void registerShutdownHook( final GraphDatabaseService graphDb )
	{
	    // Registers a shutdown hook for the Neo4j instance so that it
	    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
	    // running application).
	    Runtime.getRuntime().addShutdownHook( new Thread()
	    {
	        @Override
	        public void run()
	        {
	            graphDb.shutdown();
	        }
	    } );
	}

}
