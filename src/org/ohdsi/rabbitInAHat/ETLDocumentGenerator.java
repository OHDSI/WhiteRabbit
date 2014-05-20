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
 * 
 * @author Observational Health Data Sciences and Informatics
 * @author Martijn Schuemie
 ******************************************************************************/
package org.ohdsi.rabbitInAHat;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.zip.GZIPInputStream;

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
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		FileInputStream fileOutputStream = new FileInputStream("C:/home/Research/EMIF WP12/HCUP//HCUP_final.etl");
		GZIPInputStream gzipOutputStream = new GZIPInputStream(fileOutputStream);
		ObjectInputStream out = new ObjectInputStream(gzipOutputStream);
		ETL etl = (ETL) out.readObject();
		out.close();
		generate(etl, "C:/Users/mschuemi/Desktop/test.docx");
	}
	
	public static void generate(ETL etl, String filename) {
		try {
			CustomXWPFDocument document = new CustomXWPFDocument();
			
			addTableLevelSection(document, etl);
			
			for (Table cdmTable : etl.getCDMDatabase().getTables())
				addCDMTableSection(document, etl, cdmTable);
			
			addSourceTablesAppendix(document, etl);
			
			document.write(new FileOutputStream(new File(filename)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InvalidFormatException e) {
			e.printStackTrace();
		}
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
				if (sourceField.getValueCounts() != null)
					row.getCell(2).setText(sourceField.getValueCounts()[0][0]);
				row.getCell(3).setText(sourceField.getComment());
			}
			
		}
		
		run.setFontSize(18);
	}
	
	private static void addCDMTableSection(CustomXWPFDocument document, ETL etl, Table cdmTable) throws InvalidFormatException, FileNotFoundException {
		XWPFParagraph paragraph = document.createParagraph();
		XWPFRun run = paragraph.createRun();
		run.addBreak(BreakType.PAGE);
		
		run.setText("Table name: " + cdmTable.getName());
		run.setFontSize(18);
		
		if (!cdmTable.getComment().equals("")) {
			paragraph = document.createParagraph();
			run = paragraph.createRun();
			run.setText(cdmTable.getComment());
		}
		
		for (ItemToItemMap tableToTableMap : etl.getTableToTableMapping().getSourceToCdmMaps())
			if (tableToTableMap.getCdmItem() == cdmTable) {
				Table sourceTable = (Table) tableToTableMap.getSourceItem();
				Mapping<Field> fieldtoFieldMapping = etl.getFieldToFieldMapping(sourceTable, cdmTable);
				
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
				XWPFTable table = document.createTable(fieldtoFieldMapping.getCdmItems().size() + 1, 4);
				// table.setWidth(2000);
				XWPFTableRow header = table.getRow(0);
				setTextAndHeaderShading(header.getCell(0), "Destination Field");
				setTextAndHeaderShading(header.getCell(1), "Source Field");
				setTextAndHeaderShading(header.getCell(2), "Logic");
				setTextAndHeaderShading(header.getCell(3), "Comment");
				int rowNr = 1;
				for (MappableItem cdmField : fieldtoFieldMapping.getCdmItems()) {
					XWPFTableRow row = table.getRow(rowNr++);
					row.getCell(0).setText(cdmField.getName());
					
					StringBuilder source = new StringBuilder();
					StringBuilder logic = new StringBuilder();
					StringBuilder comment = new StringBuilder();
					for (ItemToItemMap fieldToFieldMap : fieldtoFieldMapping.getSourceToCdmMaps())
						if (fieldToFieldMap.getCdmItem() == cdmField) {
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
					for (Field field : cdmTable.getFields())
						if (field.getName().equals(cdmField.getName())) {
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
		
		tmpRun.setText("Source Data Mapping Approach");
		tmpRun.setFontSize(18);
		
		MappingPanel mappingPanel = new MappingPanel(etl.getTableToTableMapping());
		mappingPanel.setShowOnlyConnectedItems(true);
		int height = mappingPanel.getMinimumSize().height;
		mappingPanel.setSize(800, height);
		
		BufferedImage im = new BufferedImage(800, height, BufferedImage.TYPE_INT_ARGB);
		im.getGraphics().setColor(Color.WHITE);
		im.getGraphics().fillRect(0, 0, im.getWidth(), im.getHeight());
		mappingPanel.paint(im.getGraphics());
		document.addPicture(im, 600, height * 6 / 8);
	}
}
