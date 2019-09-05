/*******************************************************************************
 * Copyright 2019 Observational Health Data Sciences and Informatics
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.ohdsi.rabbitInAHat.dataModel.ETL;
import org.ohdsi.rabbitInAHat.dataModel.ETL.FileFormat;
import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.ItemToItemMap;
import org.ohdsi.rabbitInAHat.dataModel.MappableItem;
import org.ohdsi.rabbitInAHat.dataModel.Mapping;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.utilities.files.WriteTextFile;

public class ETLMarkupDocumentGenerator {

	private MarkupDocument	document;
	private ETL				etl;

	public enum DocumentType {
		MARKDOWN, HTML
	};

	public static void main(String[] args) {
		ETL etl = ETL.fromFile("c:/temp/markdown/exampleEtl.json.gz", FileFormat.GzipJson);
		ETLMarkupDocumentGenerator generator = new ETLMarkupDocumentGenerator(etl);
		generator.generate("c:/temp/markdown/index.html", DocumentType.HTML);

	}

	public ETLMarkupDocumentGenerator(ETL etl) {
		this.etl = etl;
	}

	public void generate(String fileName, DocumentType documentType) {
		try {
			if (documentType == DocumentType.HTML)
				document = new HtmlDocument(fileName);
			else
				document = new MarkdownDocument(fileName);
			addTableLevelSection();

			for (Table targetTable : etl.getTargetDatabase().getTables())
				addTargetTableSection(targetTable);

			document.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InvalidFormatException e) {
			e.printStackTrace();
		}
	}

	private void addTargetTableSection(Table targetTable) throws InvalidFormatException, IOException {
		document.addHeader2("Table name: " + targetTable.getName());

		for (ItemToItemMap tableToTableMap : etl.getTableToTableMapping().getSourceToTargetMaps())
			if (tableToTableMap.getTargetItem() == targetTable) {
				Table sourceTable = (Table) tableToTableMap.getSourceItem();
				Mapping<Field> fieldtoFieldMapping = etl.getFieldToFieldMapping(sourceTable, targetTable);

				document.addHeader3("Reading from " + tableToTableMap.getSourceItem());

				if (!tableToTableMap.getLogic().equals("")) {
					document.addParagraph(tableToTableMap.getLogic());
				}

				if (!tableToTableMap.getComment().equals("")) {
					document.addParagraph(tableToTableMap.getComment());
				}

				// Add image of field to field mapping
				MappingPanel mappingPanel = new MappingPanel(fieldtoFieldMapping);
				mappingPanel.setShowOnlyConnectedItems(true);
				int height = mappingPanel.getMinimumSize().height;
				mappingPanel.setSize(800, height);
				BufferedImage image = new BufferedImage(800, height, BufferedImage.TYPE_INT_ARGB);
				image.getGraphics().setColor(Color.WHITE);
				image.getGraphics().fillRect(0, 0, image.getWidth(), image.getHeight());
				mappingPanel.paint(image.getGraphics());
				document.addImage(image, "Field mapping");

				// Add table of field to field mapping
				List<Row> rows = new ArrayList<Row>();
				for (MappableItem targetField : fieldtoFieldMapping.getTargetItems()) {
					Row row = new Row();
					row.add("Destination Field", targetField.getName());

					StringBuilder source = new StringBuilder();
					StringBuilder logic = new StringBuilder();
					StringBuilder comment = new StringBuilder();
					for (ItemToItemMap fieldToFieldMap : fieldtoFieldMapping.getSourceToTargetMaps()) {
						if (fieldToFieldMap.getTargetItem() == targetField) {
							if (source.length() != 0)
								source.append("\n");
							source.append(fieldToFieldMap.getSourceItem().getName().trim());

							if (logic.length() != 0)
								logic.append("\n");
							logic.append(fieldToFieldMap.getLogic().trim());

							if (comment.length() != 0)
								comment.append("\n");
							comment.append(fieldToFieldMap.getComment().trim());
						}
					}
					for (Field field : targetTable.getFields()) {
						if (field.getName().equals(targetField.getName())) {
							if (comment.length() != 0)
								comment.append("\n");
							comment.append(field.getComment().trim());
						}
					}
					row.add("Source field", source.toString().trim());
					row.add("Logic", logic.toString().trim());
					row.add("Comment field", comment.toString().trim());
					rows.add(row);
				}
			
				
				document.addTable(rows);
			}
	}

	private void addTableLevelSection() throws InvalidFormatException, IOException {
		MappingPanel mappingPanel = new MappingPanel(etl.getTableToTableMapping());
		mappingPanel.setShowOnlyConnectedItems(true);
		int height = mappingPanel.getMinimumSize().height;
		mappingPanel.setSize(800, height);

		document.addHeader1(mappingPanel.getSourceDbName() + " Data Mapping Approach to " + mappingPanel.getTargetDbName());

		BufferedImage image = new BufferedImage(800, height, BufferedImage.TYPE_INT_ARGB);
		image.getGraphics().setColor(Color.WHITE);
		image.getGraphics().fillRect(0, 0, image.getWidth(), image.getHeight());
		mappingPanel.paint(image.getGraphics());
		document.addImage(image, "Table mapping");
	}

	private interface MarkupDocument {
		public void addHeader1(String header);

		public void addHeader2(String header);

		public void addHeader3(String header);

		public void addParagraph(String text);

		public void addImage(BufferedImage image, String alternative);

		public void addTable(List<Row> rows);

		public void close();
	}

	private class MarkdownDocument implements MarkupDocument {
		private List<String>	lines		= new ArrayList<String>();
		private int				imageIndex	= 0;
		private String fileName;
		private String filesFolder;
		private String mainFolder;
		
		/**
		 * 
		 * @param fileName  Full path of the markdown document to create
		 */
		public MarkdownDocument(String fileName) {
			this.fileName = fileName;
			mainFolder = new File(fileName).getParent();
			filesFolder = new File(fileName).getName().replaceAll("(\\.md)|(\\.MD)", "_files");
		}
		
		@Override
		public void addHeader1(String header) {
			lines.add("# " + header);
			lines.add("");
		}

		@Override
		public void addHeader2(String header) {
			lines.add("## " + header);
			lines.add("");
		}

		@Override
		public void addHeader3(String header) {
			lines.add("### " + header);
			lines.add("");
		}

		@Override
		public void addParagraph(String text) {
			lines.add(text);
			lines.add("");
		}

		@Override
		public void addImage(BufferedImage image, String alternative) {
			if (imageIndex == 0) {
				File folder = new File(mainFolder + "/"+ filesFolder);
				if (!folder.exists())
					folder.mkdirs();
			}
			imageIndex++;
			String imageFile = filesFolder + "/image" + imageIndex + ".png";
			try {
				ImageIO.write(image, "png", new File(mainFolder + "/" + imageFile));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			lines.add("![](" + imageFile + ")");
			lines.add("");
		}

		@Override
		public void close() {
			WriteTextFile out = new WriteTextFile(fileName);
			for (String line : lines)
				out.writeln(line);
			out.close();
		}

		@Override
		public void addTable(List<Row> rows) {
			if (rows.size() > 0) {
				String header = "| " + StringUtilities.join(rows.get(0).getFieldNames(), " | ") + " |";
				header = header.replaceAll("\n", "  ");
				lines.add(header);
				StringBuilder line = new StringBuilder();
				for (int i = 0; i < rows.get(0).getFieldNames().size(); i++)
					line.append("| --- ");
				line.append("|");
				lines.add(line.toString());

				for (Row row : rows) {
					line = new StringBuilder();
					for (String value : row.getCells())
						line.append("| " + value.replaceAll("\n", "  ") + " ");
					line.append("|");
					lines.add(line.toString());
				}
				lines.add("");
			}
		}
	}

	private class HtmlDocument implements MarkupDocument {
		private List<String>	lines		= new ArrayList<String>();
		private int				imageIndex	= 0;
		private String fileName;
		private String filesFolder;
		private String mainFolder;
		
		/**
		 * 
		 * @param fileName   Full path of the HTML file to create.
		 */
		public HtmlDocument(String fileName) {
			this.fileName = fileName;
			mainFolder = new File(fileName).getParent();
			filesFolder = new File(fileName).getName().replaceAll("(\\.html?)|(\\.html?)", "_files");
		}
		

		@Override
		public void addHeader1(String header) {
			lines.add("<h1>" + header + "</h1>");
			lines.add("");
		}

		@Override
		public void addHeader2(String header) {
			lines.add("<h2>" + header + "</h2>");
			lines.add("");
		}

		@Override
		public void addHeader3(String header) {
			lines.add("<h3>" + header + "</h3>");
			lines.add("");
		}

		@Override
		public void addParagraph(String text) {
			lines.add("<p>" + text + "</p>");
			lines.add("");
		}
		
		@Override
		public void addImage(BufferedImage image, String alternative) {
			if (imageIndex == 0) {
				File folder = new File(mainFolder + "/"+ filesFolder);
				if (!folder.exists())
					folder.mkdirs();
			}
			imageIndex++;
			String imageFile = filesFolder + "/image" + imageIndex + ".png";
			try {
				ImageIO.write(image, "png", new File(mainFolder + "/" + imageFile));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			lines.add("<img src=\"" + imageFile + "\" alt=\"" + alternative + "\">");
			lines.add("");
		}

		@Override
		public void close() {
			WriteTextFile out = new WriteTextFile(fileName);
			for (String line : lines)
				out.writeln(line);
			out.close();
		}

		@Override
		public void addTable(List<Row> rows) {
			if (rows.size() > 0) {
				lines.add("<table>");
				lines.add("\t<tr>");
				for (String fieldName : rows.get(0).getFieldNames())
					lines.add("\t\t<th>" + fieldName + "</th>");
				lines.add("\t</tr>");

				for (Row row : rows) {
					lines.add("\t<tr>");
					for (String cell : row.getCells())
						lines.add("\t\t<td>" + cell + "</td>");
					lines.add("\t</tr>");
				}
				lines.add("</table>");
				lines.add("");
			}
		}
	}
}
