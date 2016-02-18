/*******************************************************************************
 * Copyright 2016 Observational Health Data Sciences and Informatics
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
package org.ohdsi.whiteRabbit;

import java.io.File;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;

import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.WriteTextFile;

public class ErrorReport {
	public static String generate(String folder, Exception e) {
		String filename = folder + "/Error.txt";
		int i = 1;
		while (new File(filename).exists())
			filename = folder + "/Error" + (i++) + ".txt";
		DecimalFormat df = new DecimalFormat();
		DecimalFormatSymbols dfs = new DecimalFormatSymbols();
		dfs.setGroupingSeparator(',');
		df.setDecimalFormatSymbols(dfs);
		
		WriteTextFile out = new WriteTextFile(filename);
		
		out.writeln("*** Generic error information ***");
		out.writeln("Message: " + e.getMessage());
		out.writeln("Time: " + StringUtilities.now());
		Runtime runTime = Runtime.getRuntime();
		out.writeln("Processor type: " + System.getProperty("sun.cpu.isalist"));
		out.writeln("Available processors: " + runTime.availableProcessors());
		out.writeln("Maximum available memory: " + df.format(runTime.maxMemory()) + " bytes");
		out.writeln("Used memory: " + df.format(runTime.totalMemory() - runTime.freeMemory()) + " bytes");
		out.writeln("Java version: " + System.getProperty("java.version"));
		out.writeln("Java vendor: " + System.getProperty("java.vendor"));
		out.writeln("OS architecture: " + System.getProperty("os.arch"));
		out.writeln("OS name: " + System.getProperty("os.name"));
		out.writeln("OS version: " + System.getProperty("os.version"));
		out.writeln("OS patch level: " + System.getProperty("sun.os.patch.level"));
		out.writeln("");
		out.writeln("*** Stack trace ***");
		for (StackTraceElement element : e.getStackTrace())
			out.writeln(element.toString());
		out.writeln("");
		out.writeln("*** Console ***");
		out.writeln(ObjectExchange.console.getText());
		out.writeln("");
		out.writeln("*** Working folder contents ***");
		out.writeln("Directory of " + folder);
		out.writeln("");
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
		File[] files = new File(folder).listFiles();
		Arrays.sort(files, new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				return Long.valueOf(o1.lastModified()).compareTo(o2.lastModified());
			}
		});
		
		for (File file : files) {
			String name = file.getName();
			String length = df.format(file.length());
			String dir = file.isDirectory() ? "<DIR>" : "     ";
			String modifiedDate = sdf.format(new Date(file.lastModified()));
			StringBuilder filler = new StringBuilder();
			for (int x = 0; x < (80 - name.length() - length.length()); x++)
				filler.append(' ');
			out.writeln(name + filler.toString() + length + "      " + dir + "        " + modifiedDate);
		}
		out.writeln("");
		out.writeln("Available disc space: " + df.format(new File(folder).getFreeSpace()) + " bytes");
		out.close();
		return filename;
	}
}
