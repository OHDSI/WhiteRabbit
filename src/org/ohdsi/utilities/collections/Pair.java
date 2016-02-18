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
package org.ohdsi.utilities.collections;

import java.io.Serializable;

public class Pair<A, B> implements Serializable {
	
	private A					object1;
	private B					object2;
	private static final long	serialVersionUID	= 1784282381416947673L;
	
	public Pair(A item1, B item2) {
		this.object1 = item1;
		this.object2 = item2;
	}
	
	public String toString() {
		return "[[" + object1.toString() + "],[" + object2.toString() + "]]";
	}
	
	public int hashCode() {
		return object1.hashCode() + object2.hashCode();
	}
	
	public A getItem1() {
		return object1;
	}
	
	public B getItem2() {
		return object2;
	}
	
	public void setItem1(A item1) {
		object1 = item1;
	}
	
	public void setItem2(B item2) {
		object2 = item2;
	}
	
	@SuppressWarnings("rawtypes")
	public boolean equals(Object other) {
		if (other instanceof Pair)
			if (((Pair) other).object1.equals(object1))
				if (((Pair) other).object2.equals(object2))
					return true;
		return false;
	}
}
