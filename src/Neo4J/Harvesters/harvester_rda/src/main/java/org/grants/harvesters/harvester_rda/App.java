package org.grants.harvesters.harvester_rda;

public class App {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		HarvesterPmh harvester = new HarvesterPmh("http://researchdata.ands.org.au/registry/services/oai");
		
		harvester.Test();
		
	}

}
