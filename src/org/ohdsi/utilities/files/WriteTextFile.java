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

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

public class WriteTextFile {
  
  public WriteTextFile(String filename){
    FileOutputStream stream;
    try {
      stream = new FileOutputStream(filename);
      bufferedWrite = new BufferedWriter( new OutputStreamWriter(stream, "UTF-8"),10000);      
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      System.err.println("Computer does not support UTF-8 encoding");
      e.printStackTrace();
    }
  }
  
  public void writeln(String string){
    try {
      bufferedWrite.write(string);
      bufferedWrite.newLine();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public void writeln(int integer){
  	writeln(Integer.toString(integer));
  }
  
  public void writeln(Object object){
  	writeln(object.toString());
  }
  
  public void flush(){
    try {
      bufferedWrite.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public void close() {
    try {
      bufferedWrite.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  private BufferedWriter bufferedWrite;
}
