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
package org.ohdsi.utilities.files;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.List;

import org.ohdsi.utilities.StringUtilities;

public class ReadCSVFile implements Iterable<List<String>>{
  protected BufferedReader bufferedReader;
  public boolean EOF = false;
  private char delimiter = ',';

  public ReadCSVFile(String filename) {
    try {
      FileInputStream textFileStream = new FileInputStream(filename);
      bufferedReader = new BufferedReader(new InputStreamReader(textFileStream, "UTF-8"));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
  }
  
  public ReadCSVFile(InputStream inputstream){
  	try {
			bufferedReader = new BufferedReader(new InputStreamReader(inputstream, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
  }
  
  public Iterator<List<String>> getIterator() {
    return iterator();
  }

  private class CSVFileIterator implements Iterator<List<String>> {
    private String buffer;
    
    public CSVFileIterator() {
      try {
        buffer = bufferedReader.readLine();
        if (buffer == null){
          EOF = true;
          bufferedReader.close();
        }
      }
      catch (IOException e) {
        e.printStackTrace();
      }
      
    }

    public boolean hasNext() {
      return !EOF;
    }

    public List<String> next() {
      String result = buffer;
      try {
        buffer = bufferedReader.readLine();
        if (buffer == null){
          EOF = true;
          bufferedReader.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

      return line2columns(result);
    }

    public void remove() {
      System.err.println("Unimplemented method 'remove' called");
    }
  }

  public Iterator<List<String>> iterator() {
    return new CSVFileIterator();
  }
  
  private List<String> line2columns(String line){
    List<String> columns = StringUtilities.safeSplit(line, delimiter);
    for (int i = 0; i < columns.size(); i++){
      String column = columns.get(i);
      if (column.startsWith("\"") && column.endsWith("\"") && column.length() > 1)
        column = column.substring(1, column.length()-1);
      column = column.replace("\\\"", "\"");
      columns.set(i, column);
    }
    return columns;
  }

	public void setDelimiter(char delimiter) {
		this.delimiter = delimiter;
	}

	public char getDelimiter() {
		return delimiter;
	}
}
