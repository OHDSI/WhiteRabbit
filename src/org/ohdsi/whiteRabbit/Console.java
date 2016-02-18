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

import java.io.IOException;
import java.io.OutputStream;

import javax.swing.JTextArea;
import javax.swing.text.BadLocationException;

import org.ohdsi.utilities.files.WriteTextFile;

public class Console extends OutputStream {
	
	private StringBuffer	buffer	= new StringBuffer();
	private WriteTextFile	debug	= null;
	private JTextArea		textArea;
	
	public void println(String string) {
		textArea.append(string + "\n");
		textArea.repaint();
		System.out.println(string);
	}
	
	public void setTextArea(JTextArea textArea) {
		this.textArea = textArea;
	}
	
	public void setDebugFile(String filename) {
		debug = new WriteTextFile(filename);
	}
	
	public String getText() {
		try {
			return textArea.getDocument().getText(0, textArea.getDocument().getLength());
		} catch (BadLocationException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	@Override
	public void write(int b) throws IOException {
		buffer.append((char) b);
		if ((char) b == '\n') {
			if (textArea != null) {
				textArea.append(buffer.toString());
				textArea.setCaretPosition(textArea.getDocument().getLength());
			}
			if (debug != null) {
				debug.writeln(buffer.toString());
				debug.flush();
			}
			buffer = new StringBuffer();
		}
	}
	
}
