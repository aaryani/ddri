package org.vsc.harvesters.rda;

public class App {
	private static final String JSON_FOLDER_URI = "rda";

	public static void main(String[] args) {
		
		String folderJson = JSON_FOLDER_URI;
		if (args.length > 0)
			folderJson = args[0];
		
		if (folderJson.isEmpty()) {
			System.out.print( "Error: No JSON folder has been specyfied." );
			return;
		}
	
		Harvester harvester = new Harvester(folderJson);
		
		try {
			harvester.harvest();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
