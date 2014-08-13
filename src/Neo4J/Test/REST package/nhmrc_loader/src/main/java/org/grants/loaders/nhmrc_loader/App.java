package org.grants.loaders.nhmrc_loader;

import java.io.IOException;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.isbar_software.neo4j.utils.Neo4JException;

public class App {
	public static void main(String[] args) {
	    String serverUri = null;
	    if (args.length > 0)
	            serverUri = args[0];

	    Loader loader = new Loader();
	    try {
			loader.Load(serverUri);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Neo4JException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
