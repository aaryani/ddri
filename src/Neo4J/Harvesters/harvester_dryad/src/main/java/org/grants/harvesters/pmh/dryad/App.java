package org.grants.harvesters.pmh.dryad;

import java.util.List;

import org.grants.harvesters.pmh.Harvester;
import org.grants.harvesters.pmh.MetadataFormat;
import org.grants.harvesters.pmh.MetadataPrefix;

public class App {
	private static final String REPO_URI = "http://www.datadryad.org/oai/request";
	private static final String FOLDER_XML = "dryad";
	
	public static void main(String[] args) {
		
		// init server uri
		String repoUri = REPO_URI;
		if (args.length > 0)
			repoUri = args[0];
		
		// check what server uri has been supplied
		if (repoUri.isEmpty()) {
			System.out.print( "Error: No server address has been specyfied. Please provide server addres." );
			return;
		}

		String folderXml = FOLDER_XML;
		if (args.length > 1)
			folderXml = args[1];
		
		if (folderXml.isEmpty()) {
			System.out.print( "Error: No path to xml folder has been specyfied. Please provide xml folder path." );
			return;
		}
	
		Harvester harvester = new Harvester(repoUri, folderXml);
		
		List<MetadataFormat> formats = harvester.listMetadataFormats();
		System.out.println("Supported metadata formats:");
		for (MetadataFormat format : formats) {
			System.out.println(format.toString());
		}
		
		try {
			harvester.harvest(MetadataPrefix.mets);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
