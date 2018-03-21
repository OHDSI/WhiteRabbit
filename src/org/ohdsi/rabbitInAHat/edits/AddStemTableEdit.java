package org.ohdsi.rabbitInAHat.edits;

import javax.swing.undo.CannotRedoException;
import javax.swing.undo.CannotUndoException;
import javax.swing.undo.UndoableEdit;

import org.ohdsi.rabbitInAHat.dataModel.ETL;
import org.ohdsi.rabbitInAHat.dataModel.StemTableAdd;
import org.ohdsi.whiteRabbit.ObjectExchange;

public class AddStemTableEdit implements UndoableEdit{

	private ETL oldETL;
	private ETL newETL;

	public static void doEdit() {
		AddStemTableEdit addStemTableEdit = new AddStemTableEdit();
		addStemTableEdit.oldETL = new ETL(ObjectExchange.etl);
		StemTableAdd.addStemTable(ObjectExchange.etl);
		addStemTableEdit.newETL = new ETL(ObjectExchange.etl);
		ObjectExchange.tableMappingPanel.setMapping(ObjectExchange.etl.getTableToTableMapping());
		ObjectExchange.undoManager.addEdit(addStemTableEdit);

	}

	@Override
	public void undo() throws CannotUndoException {
		ObjectExchange.etl = oldETL;
		ObjectExchange.tableMappingPanel.setMapping(ObjectExchange.etl.getTableToTableMapping());
	}

	@Override
	public boolean canUndo() {
		return true;
	}

	@Override
	public void redo() throws CannotRedoException {
		ObjectExchange.etl = oldETL;
		ObjectExchange.tableMappingPanel.setMapping(ObjectExchange.etl.getTableToTableMapping());
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
		return "Add stem table";
	}

	@Override
	public String getUndoPresentationName() {
		return "Undo adding stem table";
	}

	@Override
	public String getRedoPresentationName() {
		return "Redo adding stem table";
	}
}
