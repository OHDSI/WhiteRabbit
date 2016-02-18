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
package org.ohdsi.utilities.files;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ReadTextFile implements Iterable<String>{
  public String filename;
  protected BufferedReader bufferedReader;
  public boolean EOF = false;

  
  public ReadTextFile(InputStream inputStream) {
  	 try {
			bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
      System.err.println("Computer does not support UTF-8 encoding");
			e.printStackTrace();
		}
  	
  }
  public ReadTextFile(String filename) {
    this.filename = filename;
    try {
      FileInputStream inputStream = new FileInputStream(filename);
      bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      System.err.println("Computer does not support UTF-8 encoding");
      e.printStackTrace();
    }
  }
  

  public Iterator<String> getIterator() {
    return iterator();
  }

  public List<String> loadFromFileInBatches(Integer batchsize) {
    List<String> result = new ArrayList<String>();
    if (!EOF) {
      try {
        int i = 0;
        while (!EOF && i++ < batchsize) {
          String nextLine = bufferedReader.readLine();
          if (nextLine == null)
            EOF = true;
          else
            result.add(nextLine);
        }
        if (EOF) {
          bufferedReader.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return result;
  }

  private class TextFileIterator implements Iterator<String> {
    private String buffer;
    
    public TextFileIterator() {
      try {
        buffer = bufferedReader.readLine();
        if(buffer == null) {
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

    public String next() {
      String result = buffer;
      try {
        buffer = bufferedReader.readLine();
        if(buffer == null) {
          EOF = true;
          bufferedReader.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

      return result;
    }

    public void remove() {
      // not implemented
    }

  }

  public Iterator<String> iterator() {
    return new TextFileIterator();
  }
}
