package org.vsc.harvesters.rda;

public class App {
	private static final String NEO4J_URL = "http://localhost:7474/db/data/";

	public static void main(String[] args) {
		
		String neo4jUrl = NEO4J_URL;
		if (args.length > 0)
			neo4jUrl = args[1];
		
		if (neo4jUrl.isEmpty()) {
			System.out.print( "Error: No Neo4J Server URL has been specyfied." );
			return;
		}
	
		Harvester harvester = new Harvester(neo4jUrl);
		
		try {
			harvester.harvest();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
