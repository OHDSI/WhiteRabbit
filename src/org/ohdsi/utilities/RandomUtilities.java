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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomUtilities {
	public static <T> List<T> sampleWithoutReplacement(List<T> items, int sampleSize) {
		if (sampleSize > items.size()) {
			return items;
		}
		Random random = new Random();
		List<T> sample = new ArrayList<T>();
		for (int i = 0; i < sampleSize; i++)
			sample.add(items.remove(random.nextInt(items.size())));
		return sample;
	}
	
	public static int[] sampleWithoutReplacement(int minValue, int maxValue, int sampleSize) {
		if (sampleSize > (maxValue - minValue + 1)) {
			int[] sample = new int[maxValue - minValue + 1];
			for (int i = minValue; i <= maxValue; i++)
				sample[i - minValue] = i;
			return sample;
		} else {
			Random random = new Random();
			int[] sample = new int[sampleSize];
			for (int i = 0; i < sampleSize; i++) {
				int number = random.nextInt(maxValue - minValue + 1);
				while (contains(sample, number))
					number = random.nextInt(maxValue - minValue + 1);
				sample[i] = number;
			}
			return sample;
		}
		
	}
	
	private static boolean contains(int[] sample, int number) {
		for (int i : sample)
			if (i == number)
				return true;
		return false;
	}
	
}
