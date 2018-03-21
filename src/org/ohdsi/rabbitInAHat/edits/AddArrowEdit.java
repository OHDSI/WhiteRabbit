package org.ohdsi.rabbitInAHat.edits;

import javax.swing.undo.CannotRedoException;
import javax.swing.undo.CannotUndoException;
import javax.swing.undo.UndoableEdit;

import org.ohdsi.rabbitInAHat.Arrow;
import org.ohdsi.rabbitInAHat.LabeledRectangle;
import org.ohdsi.rabbitInAHat.MappingPanel;
import org.ohdsi.rabbitInAHat.dataModel.ETL;
import org.ohdsi.rabbitInAHat.dataModel.MappableItem;
import org.ohdsi.rabbitInAHat.dataModel.StemTableAdd;
import org.ohdsi.whiteRabbit.ObjectExchange;

public class AddArrowEdit implements UndoableEdit{
	
	private ETL oldETL;
	
	public static void doEdit(boolean addUndo, MappableItem source, MappableItem target, MappingPanel mappingPanel) {
		if (addUndo) {
			AddArrowEdit addArrowEdit = new AddArrowEdit();
			ObjectExchange.undoManager.addEdit(addArrowEdit);
		}
		mappingPanel.getMapping().addSourceToTargetMap(source, target);
		Arrow arrow = new Arrow(source);
		arrow.setTarget(target);
		arrow.setItemToItemMap(mappingPanel.getMapping().getSourceToTargetMap(source.getItem(), target.getItem()));
		arrows.add(arrow);
		mappingPanel.repaint();
		StemTableAdd.addStemTable(ObjectExchange.etl);
		ObjectExchange.tableMappingPanel.setMapping(ObjectExchange.etl.getTableToTableMapping());
		
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
		doEdit(false);
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
