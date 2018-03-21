package org.ohdsi.rabbitInAHat;

import javax.swing.undo.CannotRedoException;
import javax.swing.undo.CannotUndoException;
import javax.swing.undo.UndoableEdit;

import org.ohdsi.rabbitInAHat.dataModel.ETL;
import org.ohdsi.whiteRabbit.ObjectExchange;

public class UndoableEtlEdit implements UndoableEdit{
	private ETL oldETL;
	private ETL newETL;
	private boolean tableMappingEvent;

	public UndoableEtlEdit(boolean tableMappingEvent) {
		oldETL = new ETL(ObjectExchange.etl);
		this.tableMappingEvent = tableMappingEvent;
	}
	
	public void commit() {
		newETL = new ETL(ObjectExchange.etl);
		ObjectExchange.undoManager.addEdit(this);

	}

	@Override
	public void undo() throws CannotUndoException {
		ObjectExchange.etl = oldETL;
		ObjectExchange.tableMappingPanel.renderModel();
		ObjectExchange.fieldMappingPanel.renderModel();
		if (tableMappingEvent)
			ObjectExchange.tableMappingPanel.maximize();
	}

	@Override
	public boolean canUndo() {
		return true;
	}

	@Override
	public void redo() throws CannotRedoException {
		ObjectExchange.etl = newETL;
		ObjectExchange.tableMappingPanel.renderModel();
		ObjectExchange.fieldMappingPanel.renderModel();
		if (tableMappingEvent)
			ObjectExchange.tableMappingPanel.maximize();
	}

	@Override
	public boolean canRedo() {
		return true;
	}

	@Override
	public void die() {
	}

	@Override
	public boolean addEdit(UndoableEdit anEdit) {
		return false;
	}

	@Override
	public boolean replaceEdit(UndoableEdit anEdit) {
		return false;
	}

	@Override
	public boolean isSignificant() {
		return true;
	}

	@Override
	public String getPresentationName() {
		return "";
	}

	@Override
	public String getUndoPresentationName() {
		return "Undo";
	}

	@Override
	public String getRedoPresentationName() {
		return "Redo";
	}
}
