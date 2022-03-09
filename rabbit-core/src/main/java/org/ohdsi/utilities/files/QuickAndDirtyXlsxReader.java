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
package org.ohdsi.utilities.files;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.collections.IntegerComparator;
import org.ohdsi.utilities.files.QuickAndDirtyXlsxReader.Sheet;

public class QuickAndDirtyXlsxReader extends ArrayList<Sheet> {

	private static final long serialVersionUID = 25124428448185386L;
	private static final Pattern DOUBLE_IGNORE_PATTERN = Pattern.compile("[<>= ]+");

	private final List<String> sharedStrings = new ArrayList<>();

	private final Map<String, Sheet> rIdToSheet = new HashMap<>();
	private final Map<String, Sheet> nameToSheet = new HashMap<>();
	private final Map<String, Sheet> filenameToSheet = new HashMap<>();

	public QuickAndDirtyXlsxReader(String filename) {
		try {
			// Step 1: load the shared strings (if any), and the rels
			FileInputStream inputStream = new FileInputStream(filename);
			loadSharedStringsAndRels(inputStream);

			// Step 2: load the data:
			inputStream = new FileInputStream(filename);
			readFromStream(inputStream);

			// Step 3: order the sheets:
			Collections.sort(this, (o1, o2) -> IntegerComparator.compare(o1.order, o2.order));
			inputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void loadSharedStringsAndRels(FileInputStream inputStream) {
		try {
			int tasksComplete = 0;
			ZipInputStream zipInputStream = new ZipInputStream(inputStream);

			ZipEntry zipEntry;
			while ((zipEntry = zipInputStream.getNextEntry()) != null) {
				String filename = zipEntry.getName();
				if (filename.equals("xl/sharedStrings.xml")) {
					processSharedStrings(zipInputStream);
					tasksComplete++;
				} else if (filename.equals("xl/_rels/workbook.xml.rels")) {
					processRels(zipInputStream);
					tasksComplete++;
				}

				if (tasksComplete == 2) {
					zipInputStream.close();
					break;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void processRels(ZipInputStream inputStream) throws IOException {
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
		String line;
		while ((line = bufferedReader.readLine()) != null)
			for (String tag : StringUtilities.multiFindBetween(line, "<Relationship", ">")) {
				String rId = StringUtilities.findBetween(tag, "Id=\"", "\"");
				String filename = "xl/" + StringUtilities.findBetween(tag, "Target=\"", "\"");
				if (filename.contains("/sheet")) {
					Sheet sheet = new Sheet();
					add(sheet);
					rIdToSheet.put(rId, sheet);
					filenameToSheet.put(filename, sheet);
				}
			}
	}

	private void readFromStream(InputStream inputStream) {
		try {
			ZipInputStream zipInputStream = new ZipInputStream(inputStream);

			ZipEntry zipEntry;
			while ((zipEntry = zipInputStream.getNextEntry()) != null) {
				String filename = zipEntry.getName();
				if (filename.equals("xl/workbook.xml"))
					processWorkBook(zipInputStream);
				else if (filename.startsWith("xl/worksheets/sheet"))
					processSheet(filename, zipInputStream);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void processSharedStrings(ZipInputStream inputStream) throws IOException {
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
		StringBuilder fullFile = new StringBuilder();
		String line;
		while ((line = bufferedReader.readLine()) != null)
			fullFile.append(line);

		for (String string : StringUtilities.multiFindBetween(fullFile.toString(), "<si>", "</si>"))
			if (string.trim().equals("</t>")) // Empty string
				sharedStrings.add("");
			else {
				string = StringUtilities.findBetween(string, ">", "<");
				sharedStrings.add(string);
			}
	}

	private void processSheet(String filename, ZipInputStream inputStream) throws IOException {
		Sheet sheet = filenameToSheet.get(filename);
		//System.out.println(filename + "\t" + sheet.name);
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
		String line;
		StringBuilder fullSheet = new StringBuilder();
		while ((line = bufferedReader.readLine()) != null)
			fullSheet.append(line);

		for (String rowLine : StringUtilities.multiFindBetween(fullSheet.toString(), "<row", "</row>")) {
			Row row = new Row(sheet);
			row.addAll(findCellValues(rowLine));
			if (row.size() != 0)
				sheet.add(row);
		}

	}

	public List<String> findCellValues(String string) {
		List<String> result = new ArrayList<>();
		int tagStart = -1;
		int stringStart = -1;
		int column = -1;
		boolean sharedString = false;
		for (int i = 0; i < string.length(); i++) {
			char ch = string.charAt(i);
			if (ch == '<')
				tagStart = i + 1;
			else if (ch == '>') {
				if (tagStart != -1 && i > tagStart) {
					String tag = string.substring(tagStart, i);
					if (tag.charAt(0) == 'c') {
						sharedString = tag.contains("t=\"s\"");
						column = parseColumn(StringUtilities.findBetween(tag, "r=\"", "\""));
					} else if (tag.startsWith("v") || tag.startsWith("t"))
						stringStart = i + 1;
					else if (tag.equals("/v") || tag.equals("/t")) {
						if (stringStart != -1 && i > stringStart) {
							for (int j = result.size(); j <= column; j++)
								result.add("");
							if (sharedString) {
								int index = Integer.parseInt(string.substring(stringStart, tagStart - 1));
								result.set(column, decode(sharedStrings.get(index)));
							} else
								result.set(column, decode(string.substring(stringStart, tagStart - 1)));
						}
						stringStart = -1;
						column = -1;
					}

				}
			}

		}
		return result;
	}

	private int parseColumn(String cellIdString) {
		int column = 0;
		for (int i = 0; i < cellIdString.length(); i++) {
			char ch = cellIdString.charAt(i);
			if (Character.isDigit(cellIdString.charAt(i))) {
				return column - 1;
			} else {
				column *= 26;
				column += (int) ch - 64;
			}
		}
		return -1;
	}

	private static final HashMap<String, Integer> htmlEntities = new HashMap<>();
	static {
		htmlEntities.put("nbsp", 160);
		htmlEntities.put("iexcl", 161);
		htmlEntities.put("cent", 162);
		htmlEntities.put("pound", 163);
		htmlEntities.put("curren", 164);
		htmlEntities.put("yen", 165);
		htmlEntities.put("brvbar", 166);
		htmlEntities.put("sect", 167);
		htmlEntities.put("uml", 168);
		htmlEntities.put("copy", 169);
		htmlEntities.put("ordf", 170);
		htmlEntities.put("laquo", 171);
		htmlEntities.put("not", 172);
		htmlEntities.put("shy", 173);
		htmlEntities.put("reg", 174);
		htmlEntities.put("macr", 175);
		htmlEntities.put("deg", 176);
		htmlEntities.put("plusmn", 177);
		htmlEntities.put("sup2", 178);
		htmlEntities.put("sup3", 179);
		htmlEntities.put("acute", 180);
		htmlEntities.put("micro", 181);
		htmlEntities.put("para", 182);
		htmlEntities.put("middot", 183);
		htmlEntities.put("cedil", 184);
		htmlEntities.put("sup1", 185);
		htmlEntities.put("ordm", 186);
		htmlEntities.put("raquo", 187);
		htmlEntities.put("frac14", 188);
		htmlEntities.put("frac12", 189);
		htmlEntities.put("frac34", 190);
		htmlEntities.put("iquest", 191);
		htmlEntities.put("Agrave", 192);
		htmlEntities.put("Aacute", 193);
		htmlEntities.put("Acirc", 194);
		htmlEntities.put("Atilde", 195);
		htmlEntities.put("Auml", 196);
		htmlEntities.put("Aring", 197);
		htmlEntities.put("AElig", 198);
		htmlEntities.put("Ccedil", 199);
		htmlEntities.put("Egrave", 200);
		htmlEntities.put("Eacute", 201);
		htmlEntities.put("Ecirc", 202);
		htmlEntities.put("Euml", 203);
		htmlEntities.put("Igrave", 204);
		htmlEntities.put("Iacute", 205);
		htmlEntities.put("Icirc", 206);
		htmlEntities.put("Iuml", 207);
		htmlEntities.put("ETH", 208);
		htmlEntities.put("Ntilde", 209);
		htmlEntities.put("Ograve", 210);
		htmlEntities.put("Oacute", 211);
		htmlEntities.put("Ocirc", 212);
		htmlEntities.put("Otilde", 213);
		htmlEntities.put("Ouml", 214);
		htmlEntities.put("times", 215);
		htmlEntities.put("Oslash", 216);
		htmlEntities.put("Ugrave", 217);
		htmlEntities.put("Uacute", 218);
		htmlEntities.put("Ucirc", 219);
		htmlEntities.put("Uuml", 220);
		htmlEntities.put("Yacute", 221);
		htmlEntities.put("THORN", 222);
		htmlEntities.put("szlig", 223);
		htmlEntities.put("agrave", 224);
		htmlEntities.put("aacute", 225);
		htmlEntities.put("acirc", 226);
		htmlEntities.put("atilde", 227);
		htmlEntities.put("auml", 228);
		htmlEntities.put("aring", 229);
		htmlEntities.put("aelig", 230);
		htmlEntities.put("ccedil", 231);
		htmlEntities.put("egrave", 232);
		htmlEntities.put("eacute", 233);
		htmlEntities.put("ecirc", 234);
		htmlEntities.put("euml", 235);
		htmlEntities.put("igrave", 236);
		htmlEntities.put("iacute", 237);
		htmlEntities.put("icirc", 238);
		htmlEntities.put("iuml", 239);
		htmlEntities.put("eth", 240);
		htmlEntities.put("ntilde", 241);
		htmlEntities.put("ograve", 242);
		htmlEntities.put("oacute", 243);
		htmlEntities.put("ocirc", 244);
		htmlEntities.put("otilde", 245);
		htmlEntities.put("ouml", 246);
		htmlEntities.put("divide", 247);
		htmlEntities.put("oslash", 248);
		htmlEntities.put("ugrave", 249);
		htmlEntities.put("uacute", 250);
		htmlEntities.put("ucirc", 251);
		htmlEntities.put("uuml", 252);
		htmlEntities.put("yacute", 253);
		htmlEntities.put("thorn", 254);
		htmlEntities.put("yuml", 255);
		htmlEntities.put("fnof", 402);
		htmlEntities.put("Alpha", 913);
		htmlEntities.put("Beta", 914);
		htmlEntities.put("Gamma", 915);
		htmlEntities.put("Delta", 916);
		htmlEntities.put("Epsilon", 917);
		htmlEntities.put("Zeta", 918);
		htmlEntities.put("Eta", 919);
		htmlEntities.put("Theta", 920);
		htmlEntities.put("Iota", 921);
		htmlEntities.put("Kappa", 922);
		htmlEntities.put("Lambda", 923);
		htmlEntities.put("Mu", 924);
		htmlEntities.put("Nu", 925);
		htmlEntities.put("Xi", 926);
		htmlEntities.put("Omicron", 927);
		htmlEntities.put("Pi", 928);
		htmlEntities.put("Rho", 929);
		htmlEntities.put("Sigma", 931);
		htmlEntities.put("Tau", 932);
		htmlEntities.put("Upsilon", 933);
		htmlEntities.put("Phi", 934);
		htmlEntities.put("Chi", 935);
		htmlEntities.put("Psi", 936);
		htmlEntities.put("Omega", 937);
		htmlEntities.put("alpha", 945);
		htmlEntities.put("beta", 946);
		htmlEntities.put("gamma", 947);
		htmlEntities.put("delta", 948);
		htmlEntities.put("epsilon", 949);
		htmlEntities.put("zeta", 950);
		htmlEntities.put("eta", 951);
		htmlEntities.put("theta", 952);
		htmlEntities.put("iota", 953);
		htmlEntities.put("kappa", 954);
		htmlEntities.put("lambda", 955);
		htmlEntities.put("mu", 956);
		htmlEntities.put("nu", 957);
		htmlEntities.put("xi", 958);
		htmlEntities.put("omicron", 959);
		htmlEntities.put("pi", 960);
		htmlEntities.put("rho", 961);
		htmlEntities.put("sigmaf", 962);
		htmlEntities.put("sigma", 963);
		htmlEntities.put("tau", 964);
		htmlEntities.put("upsilon", 965);
		htmlEntities.put("phi", 966);
		htmlEntities.put("chi", 967);
		htmlEntities.put("psi", 968);
		htmlEntities.put("omega", 969);
		htmlEntities.put("thetasym", 977);
		htmlEntities.put("upsih", 978);
		htmlEntities.put("piv", 982);
		htmlEntities.put("bull", 8226);
		htmlEntities.put("hellip", 8230);
		htmlEntities.put("prime", 8242);
		htmlEntities.put("Prime", 8243);
		htmlEntities.put("oline", 8254);
		htmlEntities.put("frasl", 8260);
		htmlEntities.put("weierp", 8472);
		htmlEntities.put("image", 8465);
		htmlEntities.put("real", 8476);
		htmlEntities.put("trade", 8482);
		htmlEntities.put("alefsym", 8501);
		htmlEntities.put("larr", 8592);
		htmlEntities.put("uarr", 8593);
		htmlEntities.put("rarr", 8594);
		htmlEntities.put("darr", 8595);
		htmlEntities.put("harr", 8596);
		htmlEntities.put("crarr", 8629);
		htmlEntities.put("lArr", 8656);
		htmlEntities.put("uArr", 8657);
		htmlEntities.put("rArr", 8658);
		htmlEntities.put("dArr", 8659);
		htmlEntities.put("hArr", 8660);
		htmlEntities.put("forall", 8704);
		htmlEntities.put("part", 8706);
		htmlEntities.put("exist", 8707);
		htmlEntities.put("empty", 8709);
		htmlEntities.put("nabla", 8711);
		htmlEntities.put("isin", 8712);
		htmlEntities.put("notin", 8713);
		htmlEntities.put("ni", 8715);
		htmlEntities.put("prod", 8719);
		htmlEntities.put("sum", 8721);
		htmlEntities.put("minus", 8722);
		htmlEntities.put("lowast", 8727);
		htmlEntities.put("radic", 8730);
		htmlEntities.put("prop", 8733);
		htmlEntities.put("infin", 8734);
		htmlEntities.put("ang", 8736);
		htmlEntities.put("and", 8743);
		htmlEntities.put("or", 8744);
		htmlEntities.put("cap", 8745);
		htmlEntities.put("cup", 8746);
		htmlEntities.put("int", 8747);
		htmlEntities.put("there4", 8756);
		htmlEntities.put("sim", 8764);
		htmlEntities.put("cong", 8773);
		htmlEntities.put("asymp", 8776);
		htmlEntities.put("ne", 8800);
		htmlEntities.put("equiv", 8801);
		htmlEntities.put("le", 8804);
		htmlEntities.put("ge", 8805);
		htmlEntities.put("sub", 8834);
		htmlEntities.put("sup", 8835);
		htmlEntities.put("nsub", 8836);
		htmlEntities.put("sube", 8838);
		htmlEntities.put("supe", 8839);
		htmlEntities.put("oplus", 8853);
		htmlEntities.put("otimes", 8855);
		htmlEntities.put("perp", 8869);
		htmlEntities.put("sdot", 8901);
		htmlEntities.put("lceil", 8968);
		htmlEntities.put("rceil", 8969);
		htmlEntities.put("lfloor", 8970);
		htmlEntities.put("rfloor", 8971);
		htmlEntities.put("lang", 9001);
		htmlEntities.put("rang", 9002);
		htmlEntities.put("loz", 9674);
		htmlEntities.put("spades", 9824);
		htmlEntities.put("clubs", 9827);
		htmlEntities.put("hearts", 9829);
		htmlEntities.put("diams", 9830);
		htmlEntities.put("quot", 34);
		htmlEntities.put("amp", 38);
		htmlEntities.put("lt", 60);
		htmlEntities.put("gt", 62);
		htmlEntities.put("OElig", 338);
		htmlEntities.put("oelig", 339);
		htmlEntities.put("Scaron", 352);
		htmlEntities.put("scaron", 353);
		htmlEntities.put("Yuml", 376);
		htmlEntities.put("circ", 710);
		htmlEntities.put("tilde", 732);
		htmlEntities.put("ensp", 8194);
		htmlEntities.put("emsp", 8195);
		htmlEntities.put("thinsp", 8201);
		htmlEntities.put("zwnj", 8204);
		htmlEntities.put("zwj", 8205);
		htmlEntities.put("lrm", 8206);
		htmlEntities.put("rlm", 8207);
		htmlEntities.put("ndash", 8211);
		htmlEntities.put("mdash", 8212);
		htmlEntities.put("lsquo", 8216);
		htmlEntities.put("rsquo", 8217);
		htmlEntities.put("sbquo", 8218);
		htmlEntities.put("ldquo", 8220);
		htmlEntities.put("rdquo", 8221);
		htmlEntities.put("bdquo", 8222);
		htmlEntities.put("dagger", 8224);
		htmlEntities.put("Dagger", 8225);
		htmlEntities.put("permil", 8240);
		htmlEntities.put("lsaquo", 8249);
		htmlEntities.put("rsaquo", 8250);
		htmlEntities.put("euro", 8364);
	}

	public static String decode(String s) {
		StringBuilder result = new StringBuilder(s.length());
		int ampInd = s.indexOf("&");
		int lastEnd = 0;
		while (ampInd >= 0) {
			int nextAmp = s.indexOf("&", ampInd + 1);
			int nextSemi = s.indexOf(";", ampInd + 1);
			if (nextSemi != -1 && (nextAmp == -1 || nextSemi < nextAmp)) {
				int value = -1;
				String escape = s.substring(ampInd + 1, nextSemi);
				try {
					if (escape.startsWith("#")) {
						value = Integer.parseInt(escape.substring(1), 10);
					} else {
						if (htmlEntities.containsKey(escape)) {
							value = htmlEntities.get(escape);
						}
					}
				} catch (NumberFormatException ignored) {
				}
				result.append(s, lastEnd, ampInd);
				lastEnd = nextSemi + 1;
				if (value >= 0 && value <= 0xffff) {
					result.append((char) value);
				} else {
					result.append("&").append(escape).append(";");
				}
			}
			ampInd = nextAmp;
		}
		result.append(s.substring(lastEnd));
		return result.toString();
	}

	private void processWorkBook(InputStream inputStream) throws NumberFormatException, IOException {
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			for (String sheetTag : StringUtilities.multiFindBetween(line, "<sheet ", "/>")) {
				String name = StringUtilities.findBetween(sheetTag, "name=\"", "\"");
				String order = StringUtilities.findBetween(sheetTag, "sheetId=\"", "\"");
				String rId = StringUtilities.findBetween(sheetTag, "r:id=\"", "\"");
				Sheet sheet = rIdToSheet.get(rId);
				sheet.setName(name);
				sheet.order = Integer.parseInt(order);
				nameToSheet.put(name, sheet);
			}
		}
	}

	public Sheet getByName(String sheetName) {
		return nameToSheet.get(sheetName);
	}

	public class Sheet extends ArrayList<Row> {
		private static final long	serialVersionUID	= -8597151681911998153L;
		private String				name;
		private int					order;
		private final Map<String, Integer> fieldName2ColumnIndex = new HashMap<>();

		public boolean add(Row row) {
			// Assume first row is the header, preprocess it
			if (this.size() == 0) {
				createFieldNameIndex(row);
			}
			return super.add(row);
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		private void createFieldNameIndex(List<String> row) {
			int i = 0;
			for (String header : row) {
				fieldName2ColumnIndex.put(header, i);
				i += 1;
			}
		}

		private Integer getFieldIndex(String fieldName) {
			return fieldName2ColumnIndex.get(fieldName);
		}
	}

	public class Row extends ArrayList<String> {
		private static final long	serialVersionUID	= -6391290892840364766L;
		private final Sheet sheet;

		public Row(Sheet sheet) {
			this.sheet = sheet;
		}

		/**
		 * Lookup index of the fieldName in first row of the sheet that this row belongs to.
		 * Use index to get value of this row.
		 * @param fieldName name of the field, as it appears in the header
		 * @return null if fieldName not in the header
		 */
		public String getByHeaderName(String fieldName) {
			return getStringByHeaderName(fieldName);
		}

		public String getStringByHeaderName(String fieldName) {
			// Someone may have manually deleted data, so can't assume fieldName
			// is always there:
			Integer index = sheet.getFieldIndex(fieldName);
			if (index != null && index < this.size())
				return this.get(index);
			return null;
		}

		public Double getDoubleByHeaderName(String fieldName) {
			String value = getStringByHeaderName(fieldName);
			if (value != null) {
				// Ignore operators and spaces from double values
				value = DOUBLE_IGNORE_PATTERN.matcher(value).replaceAll("");
				return Double.parseDouble(value);
			} else {
				return null;
			}
		}

		public Integer getIntByHeaderName(String fieldName) {
			Double value = getDoubleByHeaderName(fieldName);
			if (value == null) {
				return null;
			} else {
				return value.intValue();
			}
		}
	}
}
