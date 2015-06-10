/*******************************************************************************
 * Copyright 2014 Observational Health Data Sciences and Informatics
 * 
 * This file is part of WhiteRabbit
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ohdsi.rabbitInAHat;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.xwpf.usermodel.BreakType;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFRun;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableCell;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;
import org.ohdsi.ooxml.CustomXWPFDocument;
import org.ohdsi.rabbitInAHat.dataModel.ETL;
import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.ItemToItemMap;
import org.ohdsi.rabbitInAHat.dataModel.MappableItem;
import org.ohdsi.rabbitInAHat.dataModel.Mapping;
import org.ohdsi.rabbitInAHat.dataModel.Table;

public class ETLDocumentGenerator {
	
	public static void generate(ETL etl, String filename, boolean includeCounts) {
		try {
			CustomXWPFDocument document = new CustomXWPFDocument();
			
			addTableLevelSection(document, etl);
			
			for (Table targetTable : etl.getTargetDatabase().getTables())
				addTargetTableSection(document, etl, targetTable);

			if (includeCounts) addSourceTablesAppendix(document, etl);
			
			document.write(new FileOutputStream(new File(filename)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InvalidFormatException e) {
			e.printStackTrace();
		}
	}

	public static void generate(ETL etl, String filename) {
		generate(etl, filename, true);
	}
	
	private static void addSourceTablesAppendix(CustomXWPFDocument document, ETL etl) {
		XWPFParagraph paragraph = document.createParagraph();
		XWPFRun run = paragraph.createRun();
		run.addBreak(BreakType.PAGE);
		run.setText("Appendix: source tables");
		run.setFontSize(18);
		
		for (Table sourceTable : etl.getSourceDatabase().getTables()) {
			paragraph = document.createParagraph();
			run = paragraph.createRun();
			run.setText("Table: " + sourceTable.getName());
			run.setFontSize(14);
			
			if (!sourceTable.getComment().equals("")) {
				paragraph = document.createParagraph();
				run = paragraph.createRun();
				run.setText(sourceTable.getComment());
			}
			
			XWPFTable table = document.createTable(sourceTable.getFields().size() + 1, 4);
			// table.setWidth(2000);
			XWPFTableRow header = table.getRow(0);
			setTextAndHeaderShading(header.getCell(0), "Field");
			setTextAndHeaderShading(header.getCell(1), "Type");
			setTextAndHeaderShading(header.getCell(2), "Most freq. value");
			setTextAndHeaderShading(header.getCell(3), "Comment");
			int rowNr = 1;
			for (Field sourceField : sourceTable.getFields()) {
				XWPFTableRow row = table.getRow(rowNr++);
				row.getCell(0).setText(sourceField.getName());
				row.getCell(1).setText(sourceField.getType());
				if (sourceField.getValueCounts() != null && sourceField.getValueCounts().length != 0)
					row.getCell(2).setText(sourceField.getValueCounts()[0][0]);
				row.getCell(3).setText(sourceField.getComment());
			}
			
		}
		
		run.setFontSize(18);
	}
	
	private static void addTargetTableSection(CustomXWPFDocument document, ETL etl, Table targetTable) throws InvalidFormatException, FileNotFoundException {
		XWPFParagraph paragraph = document.createParagraph();
		XWPFRun run = paragraph.createRun();
		run.addBreak(BreakType.PAGE);
		
		run.setText("Table name: " + targetTable.getName());
		run.setFontSize(18);
		
		if (!targetTable.getComment().equals("")) {
			paragraph = document.createParagraph();
			run = paragraph.createRun();
			run.setText(targetTable.getComment());
		}
		
		for (ItemToItemMap tableToTableMap : etl.getTableToTableMapping().getSourceToTargetMaps())
			if (tableToTableMap.getTargetItem() == targetTable) {
				Table sourceTable = (Table) tableToTableMap.getSourceItem();
				Mapping<Field> fieldtoFieldMapping = etl.getFieldToFieldMapping(sourceTable, targetTable);
				
				paragraph = document.createParagraph();
				run = paragraph.createRun();
				run.setText("Reading from " + tableToTableMap.getSourceItem());
				run.setFontSize(14);
				
				if (!tableToTableMap.getLogic().equals("")) {
					paragraph = document.createParagraph();
					run = paragraph.createRun();
					run.setText(tableToTableMap.getLogic());
				}
				
				if (!tableToTableMap.getComment().equals("")) {
					paragraph = document.createParagraph();
					run = paragraph.createRun();
					run.setText(tableToTableMap.getComment());
				}
				
				// Add picture of field to field mapping
				MappingPanel mappingPanel = new MappingPanel(fieldtoFieldMapping);
				mappingPanel.setShowOnlyConnectedItems(true);
				int height = mappingPanel.getMinimumSize().height;
				mappingPanel.setSize(800, height);
				
				BufferedImage im = new BufferedImage(800, height, BufferedImage.TYPE_INT_ARGB);
				im.getGraphics().setColor(Color.WHITE);
				im.getGraphics().fillRect(0, 0, im.getWidth(), im.getHeight());
				mappingPanel.paint(im.getGraphics());
				document.addPicture(im, 600, height * 6 / 8);
				
				// Add table of field to field mapping
				XWPFTable table = document.createTable(fieldtoFieldMapping.getTargetItems().size() + 1, 4);
				// table.setWidth(2000);
				XWPFTableRow header = table.getRow(0);
				setTextAndHeaderShading(header.getCell(0), "Destination Field");
				setTextAndHeaderShading(header.getCell(1), "Source Field");
				setTextAndHeaderShading(header.getCell(2), "Logic");
				setTextAndHeaderShading(header.getCell(3), "Comment");
				int rowNr = 1;
				for (MappableItem targetField : fieldtoFieldMapping.getTargetItems()) {
					XWPFTableRow row = table.getRow(rowNr++);
					row.getCell(0).setText(targetField.getName());
					
					StringBuilder source = new StringBuilder();
					StringBuilder logic = new StringBuilder();
					StringBuilder comment = new StringBuilder();
					for (ItemToItemMap fieldToFieldMap : fieldtoFieldMapping.getSourceToTargetMaps())
						if (fieldToFieldMap.getTargetItem() == targetField) {
							if (source.length() != 0)
								source.append("\n");
							source.append(fieldToFieldMap.getSourceItem().getName());
							
							if (logic.length() != 0)
								logic.append("\n");
							logic.append(fieldToFieldMap.getLogic());
							
							if (comment.length() != 0)
								comment.append("\n");
							comment.append(fieldToFieldMap.getComment());
						}
					for (Field field : targetTable.getFields())
						if (field.getName().equals(targetField.getName())) {
							if (comment.length() != 0)
								comment.append("\n");
							comment.append(field.getComment());
						}
					
					row.getCell(1).setText(source.toString());
					row.getCell(2).setText(logic.toString());
					row.getCell(3).setText(comment.toString());
				}
			}
		
	}
	
	private static void setTextAndHeaderShading(XWPFTableCell cell, String text) {
		cell.setText(text);
		
		cell.setColor("AAAAFF");
		// CTShd ctshd = cell.getCTTc().addNewTcPr().addNewShd();
		// ctshd.setColor("FFFFFF");
		// ctshd.setVal(STShd.CLEAR);
		// ctshd.setFill("6666BB");
		
	}
	
	private static void addTableLevelSection(CustomXWPFDocument document, ETL etl) throws InvalidFormatException, FileNotFoundException {
		XWPFParagraph tmpParagraph = document.createParagraph();
		XWPFRun tmpRun = tmpParagraph.createRun();
		
		MappingPanel mappingPanel = new MappingPanel(etl.getTableToTableMapping());
		mappingPanel.setShowOnlyConnectedItems(true);
		int height = mappingPanel.getMinimumSize().height;
		mappingPanel.setSize(800, height);
		
		tmpRun.setText(mappingPanel.getSourceDbName() + " Data Mapping Approach to " + mappingPanel.getTargetDbName());
		tmpRun.setFontSize(18);
		
		BufferedImage im = new BufferedImage(800, height, BufferedImage.TYPE_INT_ARGB);
		im.getGraphics().setColor(Color.WHITE);
		im.getGraphics().fillRect(0, 0, im.getWidth(), im.getHeight());
		mappingPanel.paint(im.getGraphics());
		document.addPicture(im, 600, height * 6 / 8);
	}
}
