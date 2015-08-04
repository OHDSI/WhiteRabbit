package org.ohdsi.rabbitInAHat.dataModel;

import java.util.List;

public class TableSet implements MappableItem {

	private Database			db;
	private List<Table>			tables;
	private static final long	serialVersionUID	= -5679887756890719269L;
	
	public TableSet(List<Table>	tables){
		this.tables = tables;
	}

	@Override
	public String outputName() {
		return "";
	}

	@Override
	public String getName() {
		return "";
	}

	@Override
	public Database getDb() {
		return db;
	}

	public List<Table> getTables() {
		return tables;
	}

}
