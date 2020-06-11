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
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

import org.ohdsi.rabbitInAHat.dataModel.ETL;
import org.ohdsi.rabbitInAHat.dataModel.ETL.FileFormat;
import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.ItemToItemMap;
import org.ohdsi.rabbitInAHat.dataModel.MappableItem;
import org.ohdsi.rabbitInAHat.dataModel.Mapping;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.Row;

public class ETLMarkupDocumentGenerator {

	private MarkupDocument document;
	private List<String> targetTablesWritten;
	private ETL etl;

	public enum DocumentType {
		MARKDOWN, HTML
	}

	public static void main(String[] args) {
		ETL etl = ETL.fromFile("c:/temp/markdown/exampleEtl.json.gz", FileFormat.GzipJson);
		ETLMarkupDocumentGenerator generator = new ETLMarkupDocumentGenerator(etl);
		generator.generate("c:/temp/markdown/index.html", DocumentType.HTML);
	}

	ETLMarkupDocumentGenerator(ETL etl) {
		this.etl = etl;
		this.targetTablesWritten = new ArrayList<>();
	}

	void generate(String fileName, DocumentType documentType) {
		if (documentType == DocumentType.HTML) {
			document = new HtmlDocument(fileName);
		} else {
			document = new MarkdownDocument(fileName);
		}

		for (Table targetTable : etl.getTargetDatabase().getTables()) {
			boolean targetTableHasMapping = addTargetTableSection(targetTable);

			if (targetTableHasMapping) {
				document.write(targetTable.getName());
				targetTablesWritten.add(targetTable.getName());
			} else {
				document.reset();
			};
		}

		document.addHeader1("Appendix: source tables");
		for (Table sourceTable : etl.getSourceDatabase().getTables()) {
			addSourceTablesAppendix(sourceTable);
		}
		document.write("source_appendix");
		targetTablesWritten.add("source_appendix");

		addTableLevelSection();
		document.write("index");
	}

	private void addTableLevelSection() {
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

		document.addHeader2("Contents");
		for (String targetTableName : targetTablesWritten) {
			document.addFileLink(targetTableName);
		}
	}

	private boolean addTargetTableSection(Table targetTable) {
		document.addHeader2("Table name: " + targetTable.getName());

		boolean hasMappings = false;
		for (ItemToItemMap tableToTableMap : etl.getTableToTableMapping().getSourceToTargetMaps())
			if (tableToTableMap.getTargetItem() == targetTable) {
				hasMappings = true;
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
				List<Row> rows = new ArrayList<>();
				for (MappableItem targetField : fieldtoFieldMapping.getTargetItems()) {
					Row row = new Row();
					row.add("Destination Field", targetField.getName());

					StringBuilder source = new StringBuilder();
					StringBuilder logic = new StringBuilder();
					StringBuilder comment = new StringBuilder();
					for (ItemToItemMap fieldToFieldMap : fieldtoFieldMapping.getSourceToTargetMaps()) {
						if (fieldToFieldMap.getTargetItem() == targetField) {
							if (source.length() != 0)
								source.append("<br>");
							source.append(fieldToFieldMap.getSourceItem().getName().trim());

							if (logic.length() != 0)
								logic.append("<br>");
							logic.append(fieldToFieldMap.getLogic().trim());

							if (comment.length() != 0)
								comment.append("<br>");
							comment.append(fieldToFieldMap.getComment().trim());
						}
					}
					for (Field field : targetTable.getFields()) {
						if (field.getName().equals(targetField.getName())) {
							if (comment.length() != 0)
								comment.append("<br>");
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
		return hasMappings;
	}

	private void addSourceTablesAppendix(Table sourceTable) {
		document.addHeader3("Table: " + sourceTable.getName());

		List<Row> rows = new ArrayList<>();
		for (Field field : sourceTable.getFields()) {
			String mostFrequentValue = field.getValueCounts().getMostFrequentValue();

			Row row = new Row();
			row.add("Field", field.getName());
			row.add("Type", field.getType());
			row.add("Most freq. value", mostFrequentValue);
			row.add("Comment", field.getComment());
			rows.add(row);
		}
		document.addTable(rows);
	}

	private interface MarkupDocument {
		void addHeader1(String header);

		void addHeader2(String header);

		void addHeader3(String header);

		void addParagraph(String text);

		void addImage(BufferedImage image, String alternative);

		void addFileLink(String targetName);

		void addTable(List<Row> rows);

		void write(String targetName);

		void reset();
	}

	private static class MarkdownDocument implements MarkupDocument {
		private List<String> lines = new ArrayList<>();
		private int imageIndex = 0;
		private File mainFolder;
		private File filesFolder;

		/**
		 * 
		 * @param directoryName  Full path of the markdown document to create
		 */
		MarkdownDocument(String directoryName) {
			this.mainFolder = new File(directoryName);
			this.filesFolder = new File(directoryName, "md_files");
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
			if (imageIndex == 0 && !filesFolder.exists()) {
				filesFolder.mkdirs();
			}
			imageIndex++;
			File imageFile = new File(filesFolder, "image" + imageIndex + ".png");
			try {
				ImageIO.write(image, "png", imageFile);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			lines.add("![](" + filesFolder.getName() + "/" + imageFile.getName() + ")");
			lines.add("");
		}

		public void addFileLink(String targetName) {
			lines.add(String.format("[%1$s](%1$s.md)", targetName));
			lines.add("");
		}

		@Override
		public void write(String targetName) {
			File outFile = new File(mainFolder, targetName + ".md");
			try (BufferedWriter bw = Files.newBufferedWriter(outFile.toPath());
				 PrintWriter out = new PrintWriter(bw)) {
				for (String line : lines) {
					out.println(line);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			this.reset();
		}

		@Override
		public void reset() {
			lines = new ArrayList<>();
		}

		@Override
		public void addTable(List<Row> rows) {
			if (rows.size() == 0) {
				return;
			}

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
				for (String value : row.getCells()) {
					if (value != null) {
						value = value.replaceAll("\n", "  ");
					} else {
						value = "";
					}
					line.append("| ").append(value).append(" ");
				}
				line.append("|");
				lines.add(line.toString());
			}
			lines.add("");
		}
	}

	private static class HtmlDocument implements MarkupDocument {
		private List<String> lines = new ArrayList<>();
		private int	imageIndex = 0;
		private File mainFolder;
		private File filesFolder;

		/**
		 * 
		 * @param directoryName   Full path of the HTML file to create.
		 */
		HtmlDocument(String directoryName) {
			this.mainFolder = new File(directoryName);
			this.filesFolder = new File(directoryName, "html_files");
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
			if (imageIndex == 0 && !filesFolder.exists()) {
				filesFolder.mkdirs();
			}
			imageIndex++;
			File imageFile = new File(filesFolder, "image" + imageIndex + ".png");
			try {
				ImageIO.write(image, "png", imageFile);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			lines.add("<img src=\"" + filesFolder.getName() + "/" + imageFile.getName() + "\" alt=\"" + alternative + "\">");
			lines.add("");
		}

		public void addFileLink(String targetName) {
			lines.add(String.format("<ul><a href=\"%1$s.html\">%1$s</a></ul>", targetName));
			lines.add("");
		}

		@Override
		public void write(String targetName) {
			File outFile = new File(mainFolder, targetName + ".html");
			try (BufferedWriter bw = Files.newBufferedWriter(outFile.toPath());
				 PrintWriter out = new PrintWriter(bw)) {
				for (String line : lines) {
					out.println(line);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

			this.reset();
		}

		@Override
		public void reset() {
			lines = new ArrayList<>();
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
					for (String cell : row.getCells()) {
						if (cell == null) {
							cell = "";
						}
						lines.add("\t\t<td>" + cell + "</td>");
					}
					lines.add("\t</tr>");
				}
				lines.add("</table>");
				lines.add("");
			}
		}
	}
}
