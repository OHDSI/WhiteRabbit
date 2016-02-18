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
package org.ohdsi.utilities;

public class SimpleCounter {
	private int		reportN;
	private long	count;
	private long	lastTime;
	private long	lastCount;
	private boolean	reportRate	= false;
	
	public SimpleCounter(int reportN) {
		this.reportN = reportN;
		count = 0;
		lastCount = 0;
		lastTime = System.currentTimeMillis();
	}
	
	public SimpleCounter(int reportN, boolean reportRate){
		this(reportN);
		setReportRate(reportRate);
	}
	
	public void count() {
		count++;
		if (count % reportN == 0)
			report();
	}
	
	private void report() {
		if (reportRate){
			long interval = System.currentTimeMillis() - lastTime;
			long processed = count - lastCount;
			System.out.println(count + " (time per unit = " + interval/(double)processed + "ms)");
			lastTime = System.currentTimeMillis();
			lastCount = count;
		} else
			System.out.println(count);
	}
	
	public void finish() {
		report();
	}
	
	public long getCount() {
		return count;
	}
	
	public boolean isReportRate() {
		return reportRate;
	}
	
	public void setReportRate(boolean value) {
		this.reportRate = value;
	}
}
