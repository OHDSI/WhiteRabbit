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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.zip.DataFormatException;

public class StringUtilities {
	
	public static long		SECOND			= 1000;
	public static long		MINUTE			= 60 * SECOND;
	public static long		HOUR			= 60 * MINUTE;
	public static long		DAY				= 24 * HOUR;
	public static long		WEEK			= 7 * DAY;
	public static long		YEAR			= 365 * DAY;
	public static long		CENTURY			= 100 * YEAR;
	public static long		MILLENIUM		= 1000 * YEAR;
	public static long		MISSING_DATE	= -999999;
	
	private static Calendar	calendar		= new GregorianCalendar();
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static String joinSorted(Collection<? extends Comparable> s, String delimiter) {
		List list = new ArrayList(s);
		Collections.sort(list);
		return join(list, delimiter);
	}
	
	public static String join(Collection<?> s, String delimiter) {
		StringBuffer buffer = new StringBuffer();
		Iterator<?> iter = s.iterator();
		if (iter.hasNext()) {
			buffer.append(iter.next().toString());
		}
		while (iter.hasNext()) {
			buffer.append(delimiter);
			buffer.append(iter.next().toString());
		}
		return buffer.toString();
	}
	
	public static String join(Object[] objects, String delimiter) {
		StringBuffer buffer = new StringBuffer();
		if (objects.length != 0)
			buffer.append(objects[0].toString());
		for (int i = 1; i < objects.length; i++) {
			buffer.append(delimiter);
			buffer.append(objects[i].toString());
		}
		return buffer.toString();
	}
	
	public static int twoHexDigitsToInt(String value, int index) {
		return Integer.parseInt(value.substring(index, index + 2), 16);
	}
	
	public static boolean isInteger(String string) {
		try {
			Integer.parseInt(string);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}
	
	public static boolean isLong(String string) {
		try {
			Long.parseLong(string);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}
	
	// private static Pattern numberPattern = Pattern.compile("^-?\\d[0-9]*(\\.[0-9]+)?((E|e)-?[0-9]*)?\\d$");
	
	public static boolean isNumber(String string) {
		try {
			Double.parseDouble(string);
		} catch (Exception e) {
			return false;
		}
		return true;
		// string.trim();
		// if (string.length() == 1) {
		// return Character.isDigit(string.charAt(0));
		// }
		// return numberPattern.matcher(string).matches();
	}
	
	public static boolean isRomanNumeral(String string) {
		return (string.equals("I") || string.equals("II") || string.equals("III") || string.equals("IV") || string.equals("V") || string.equals("VI")
				|| string.equals("VII") || string.equals("VIII") || string.equals("IX") || string.equals("IX"));
	}
	
	public static boolean isGreekLetter(String string) {
		String lcstring = string.toLowerCase();
		return (lcstring.equals("alpha") || lcstring.equals("beta") || lcstring.equals("gamma") || lcstring.equals("delta") || lcstring.equals("epsilon")
				|| lcstring.equals("zeta") || lcstring.equals("eta") || lcstring.equals("theta") || lcstring.equals("iota") || lcstring.equals("kappa")
				|| lcstring.equals("lambda") || lcstring.equals("mu") || lcstring.equals("nu") || lcstring.equals("xi") || lcstring.equals("omicron")
				|| lcstring.equals("pi") || lcstring.equals("rho") || lcstring.equals("sigma") || lcstring.equals("tau") || lcstring.equals("upsilon")
				|| lcstring.equals("phi") || lcstring.equals("chi") || lcstring.equals("psi") || lcstring.equals("omega"));
	}
	
	// Adds PSF file specific escape characters to string
	// Author: Martijn
	public static String escape(String string) {
		StringBuffer result = new StringBuffer();
		for (int i = 0; i < string.length(); i++) {
			char currentChar = string.charAt(i);
			if (currentChar == '"' || currentChar == '?' || currentChar == ';' || currentChar == '\\' || currentChar == '|') {
				result.append('\\');
			}
			result.append(currentChar);
		}
		return result.toString();
	}
	
	// Removes any escape characters from string
	// Author: Martijn
	public static String unescape(String string) {
		StringBuffer result = new StringBuffer();
		if (string.length() > 0) {
			if (string.charAt(0) == '"' && string.charAt(string.length() - 1) == '"') {
				result.append(string.substring(1, string.length() - 1));
			} else {
				boolean escape = false;
				char currentchar;
				for (int i = 0; i < string.length(); i++) {
					currentchar = string.charAt(i);
					if (escape) {
						escape = false;
						result.append(currentchar);
					} else {
						if (currentchar == '\\') {
							escape = true;
						} else {
							result.append(currentchar);
						}
					}
				}
			}
		}
		return result.toString();
	}
	
	// Safesplit works the same as default split, but takes escapes into account
	// Author: Martijn
	public static List<String> safeSplit(String string, char delimiter) {
		List<String> result = new ArrayList<String>();
		if (string.length() == 0) {
			result.add("");
			return result;
		}
		boolean literal = false;
		boolean escape = false;
		int startpos = 0;
		int i = 0;
		char currentchar;
		while (i < string.length()) {
			currentchar = string.charAt(i);
			if (currentchar == '"' && !escape) {
				literal = !literal;
			}
			if (!literal && (currentchar == delimiter && !escape)) {
				result.add(string.substring(startpos, i));
				startpos = i + 1;
			}
			if (currentchar == '\\') {
				escape = !escape;
			} else {
				escape = false;
			}
			i++;
		}
		result.add(string.substring(startpos, i));
		return result;
	}
	
	public static boolean containsNumber(String string) {
		for (int i = 0; i < string.length(); i++) {
			if ((int) string.charAt(i) < 58 && (int) string.charAt(i) > 47) {
				return true;
			}
		}
		return false;
	}
	
	public static int countNumbers(String string) {
		int total = 0;
		for (int i = 0; i < string.length(); i++) {
			if ((int) string.charAt(i) < 58 && (int) string.charAt(i) > 47) {
				total++;
			}
		}
		return total;
	}
	
	public static boolean containsLetter(String string) {
		for (int i = 0; i < string.length(); i++) {
			if (Character.isLetter(string.charAt(i))) {
				return true;
			}
		}
		return false;
	}
	
	public static int countLetters(String string) {
		int total = 0;
		for (int i = 0; i < string.length(); i++) {
			if (Character.isLetter(string.charAt(i))) {
				total++;
			}
		}
		return total;
	}
	
	public static boolean containsCurlyBracket(String string) {
		for (int i = 0; i < string.length(); i++) {
			if (isCurlyBracket(string.charAt(i))) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean containsParenthesis(String string) {
		for (int i = 0; i < string.length(); i++) {
			if (isParenthesis(string.charAt(i))) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean containsBracket(String string) {
		for (int i = 0; i < string.length(); i++) {
			if (isBracket(string.charAt(i))) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean containsArrow(String string) {
		for (int i = 0; i < string.length(); i++) {
			if (isArrow(string.charAt(i))) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean isParenthesis(char ch) {
		return (ch == ('(') || ch == (')'));
	}
	
	// Checks whether the word is a brackets
	// Author: Kristina
	public static boolean isBracket(char ch) {
		return (ch == ('[') || ch == (']'));
	}
	
	public static boolean isArrow(char ch) {
		return (ch == ('<') || ch == ('>'));
	}
	
	// Checks whether the word is a curly bracket
	// Author: Kristina
	public static boolean isCurlyBracket(char ch) {
		return (ch == ('{') || ch == ('}'));
	}
	
	// Converts a string to a list of words
	// Author: Martijn
	public static List<String> mapToWords(String string) {
		List<String> result = new ArrayList<String>();
		
		int start = 0;
		int i = 0;
		for (; i < string.length(); i++) {
			char ch = string.charAt(i);
			if (!Character.isLetterOrDigit(ch)
					&& !(ch == '\'' && i > 0 && Character.isLetter(string.charAt(i - 1)) && string.length() - 1 > i && string.charAt(i + 1) == 's' && (string
							.length() - 2 == i || !Character.isLetterOrDigit(string.charAt(i + 2))))) { // leaves ' in possesive pattern
				if (start != i) {
					result.add(string.substring(start, i));
				}
				start = i + 1;
			}
		}
		if (start != i) {
			result.add(string.substring(start, i));
		}
		return result;
	}
	
	// Returns a string with the current time
	// Author: Martijn
	public static String now() {
		Date d = new Date();
		DateFormat df = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.MEDIUM);
		return df.format(d);
	}
	
	public static void outputWithTime(String message) {
		System.out.println(now() + "\t" + message);
	}
	
	// Checks whether the word is an abbreviation
	// Author: Martijn
	public static boolean isAbbr(String word) {
		int lowercase = 0;
		int uppercase = 0;
		int charInt = 0;
		for (int i = 0; i < word.length(); i++) {
			charInt = (int) word.charAt(i);
			if (charInt < 58) {
				if (charInt > 47) {
				}// its a number
			} else if (charInt < 91) {
				if (charInt > 64) {
					uppercase++;
				}
			} else if (charInt < 123 && charInt > 96) {
				lowercase++;
			}
		}
		return (uppercase > 0 && lowercase < uppercase);
	}
	
	/**
	 * If only the first letter of a word is a capital, the word is reduced to lowercase, else the original string is returned
	 * 
	 * @param string
	 * @return
	 */
	public static String firstLetterToLowerCase(String string) {
		boolean uppercase = false;
		int charInt = 0;
		for (int i = 1; i < string.length(); i++) {
			charInt = (int) string.charAt(i);
			if (charInt < 91)
				if (charInt > 64) {
					uppercase = true;
					break;
				}
		}
		if (!uppercase)
			return string.toLowerCase();
		else
			return string;
	}
	
	public static int countsCharactersInUpperCase(String string) {
		int uppercase = 0;
		int charInt = 0;
		for (int i = 0; i < string.length(); i++) {
			charInt = (int) string.charAt(i);
			if (charInt > 64 && charInt < 91) {
				uppercase++;
			}
		}
		return uppercase;
	}
	
	public static int countsCharactersInLowerCase(String string) {
		int lowercase = 0;
		int charInt = 0;
		for (int i = 0; i < string.length(); i++) {
			charInt = (int) string.charAt(i);
			if (charInt > 96 && charInt < 123) {
				lowercase++;
			}
		}
		return lowercase;
	}
	
	// Converts a double to a formatted string. Examples of valid patterns are:
	// "###,###.###"
	// "###.##"
	// "000000.000"
	// "$###,###.###"
	// "\u00a5###,###.###"
	// # indicates optional number, 0 indicates forced number (will be printed as 0 when 0)
	// Author: Martijn
	public static String formatNumber(String pattern, double number) {
		DecimalFormat myFormatter = new DecimalFormat(pattern);
		return myFormatter.format(number);
	}
	
	public static boolean isPlural(String string) {
		if (string.length() > 1)
			if (string.charAt(string.length() - 1) == 's')
				if (Character.isLetter(string.charAt(string.length() - 2)))
					return true;
		return false;
	}
	
	public static String findBetween(String source, String pre, String post) {
		int start = source.indexOf(pre);
		if (start == -1)
			return "";
		int end = source.indexOf(post, start + pre.length());
		if (end == -1)
			return "";
		return source.substring(start + pre.length(), end);
	}
	
	public static List<String> multiFindBetween(String source, String pre, String post) {
		List<String> result = new ArrayList<String>();
		int start = 0;
		int end = 0;
		while (start != -1 && end != -1) {
			start = source.indexOf(pre, end);
			if (start != -1) {
				end = source.indexOf(post, start + pre.length());
				if (end != -1)
					result.add(source.substring(start + pre.length(), end));
			}
		}
		return result;
	}
	
	/**
	 * Returns true if every parenthesis in the string is matched
	 * 
	 * @param string
	 * @return
	 */
	public static boolean parenthesisMatch(String string) {
		int count = 0;
		for (int i = 0; i < string.length(); i++) {
			char ch = string.charAt(i);
			if (ch == '(')
				count++;
			else if (ch == ')') {
				count--;
				if (count == -1)
					return false;
			}
		}
		return (count == 0);
	}
	
	public static int count(String s, char ch) {
		int cnt = 0;
		for (int i = 0; i < s.length(); i++)
			if (s.charAt(i) == ch)
				cnt++;
		return cnt;
	}
	
	/**
	 * Removes parenthesis and what is within the parenthesis from the string. For example: 'cold (disease)' -> 'cold '
	 * 
	 * @param string
	 * @return
	 */
	public static String removeParenthesisAndContent(String string) {
		StringBuilder result = new StringBuilder();
		int count = 0;
		for (int i = 0; i < string.length(); i++) {
			char ch = string.charAt(i);
			if (ch == '(')
				count++;
			else if (ch == ')') {
				count--;
			} else if (count == 0)
				result.append(ch);
		}
		return result.toString();
	}
	
	public static String daysToSortableDateString(long days) {
		long ms = days * DAY;
		// Calendar calendar = new GregorianCalendar();
		ms -= calendar.getTimeZone().getOffset(ms);
		calendar.setTimeInMillis(ms);
		StringBuilder sb = new StringBuilder();
		sb.append(calendar.get(Calendar.YEAR));
		sb.append(StringUtilities.formatNumber("00", calendar.get(Calendar.MONTH) + 1));
		sb.append(StringUtilities.formatNumber("00", calendar.get(Calendar.DATE)));
		return sb.toString();
	}
	
	public static String daysToDatabaseDateString(long days) {
		if (days == MISSING_DATE)
			return "";
		long ms = days * DAY;
		ms -= calendar.getTimeZone().getOffset(ms);
		calendar.setTimeInMillis(ms);
		StringBuilder sb = new StringBuilder();
		sb.append(calendar.get(Calendar.YEAR));
		sb.append("-");
		sb.append(StringUtilities.formatNumber("00", calendar.get(Calendar.MONTH) + 1));
		sb.append("-");
		sb.append(StringUtilities.formatNumber("00", calendar.get(Calendar.DATE)));
		return sb.toString();
	}
	
	public static String daysToCalendarYear(long days) {
		long ms = days * DAY;
		ms -= calendar.getTimeZone().getOffset(ms);
		calendar.setTimeInMillis(ms);
		return Integer.toString(calendar.get(Calendar.YEAR));
	}
	
	public static String dateToCalendarYear(Date date) {
		calendar.setTime(date);
		return Integer.toString(calendar.get(Calendar.YEAR));
	}
	
	public static String daysToCalendarMonth(long days) {
		long ms = days * DAY;
		ms -= calendar.getTimeZone().getOffset(ms);
		calendar.setTimeInMillis(ms);
		return Integer.toString(calendar.get(Calendar.MONTH) + 1);
	}
	
	public static String daysToCalendarDayOfMonth(long days) {
		long ms = days * DAY;
		ms -= calendar.getTimeZone().getOffset(ms);
		calendar.setTimeInMillis(ms);
		return Integer.toString(calendar.get(Calendar.DATE) + 1);
	}
	
	public static String daysToCalendarQuarterYear(long days) {
		long ms = days * DAY;
		ms -= calendar.getTimeZone().getOffset(ms);
		calendar.setTimeInMillis(ms);
		return Integer.toString(1 + (calendar.get(Calendar.MONTH) / 3));
	}
	
	public static String millisecondsToSortableTimeString(long ms) {
		// Calendar calendar = new GregorianCalendar();
		ms -= calendar.getTimeZone().getOffset(ms + 2 * HOUR);
		calendar.setTimeInMillis(ms);
		StringBuilder sb = new StringBuilder();
		sb.append(calendar.get(Calendar.YEAR));
		sb.append(StringUtilities.formatNumber("00", calendar.get(Calendar.MONTH) + 1));
		sb.append(StringUtilities.formatNumber("00", calendar.get(Calendar.DATE)));
		sb.append(StringUtilities.formatNumber("00", calendar.get(Calendar.HOUR)));
		sb.append(StringUtilities.formatNumber("00", calendar.get(Calendar.MINUTE)));
		sb.append(StringUtilities.formatNumber("00", calendar.get(Calendar.SECOND)));
		return sb.toString();
	}
	
	public static long sortableTimeStringToDays(String string) throws DataFormatException {
		// Calendar calendar = new GregorianCalendar();
		try {
			int year = Integer.parseInt(string.substring(0, 4));
			int month = Integer.parseInt(string.substring(4, 6)) - 1;
			int day = Integer.parseInt(string.substring(6, 8));
			calendar.set(year, month, day);
			long time = calendar.getTimeInMillis();
			time += calendar.getTimeZone().getOffset(time);
			
			if (string.length() > 8) {
				int hour = Integer.parseInt(string.substring(8, 10));
				time += hour * 60 * 60 * 1000;
				if (string.length() > 8) {
					int minute = Integer.parseInt(string.substring(10, 12));
					time += minute * 60 * 1000;
					if (string.length() > 8) {
						int second = Integer.parseInt(string.substring(12, 14));
						time += second * 1000;
					}
				}
			}
			// Millenium is added because for negative numbers, integer division truncates upwards! (-8/10 = 0)
			return (((MILLENIUM + time) / DAY) - (1000 * 365));
		} catch (Exception e) {
			throw new DataFormatException("Error parsing date: \"" + string + "\"");
		}
	}
	
	public static long databaseTimeStringToDays(String string) {
		if (string.equals(""))
			return MISSING_DATE;
		int year = Integer.parseInt(string.substring(0, 4));
		int month = Integer.parseInt(string.substring(5, 7)) - 1;
		int day = Integer.parseInt(string.substring(8, 10));
		calendar.set(year, month, day);
		long time = calendar.getTimeInMillis();
		time += calendar.getTimeZone().getOffset(time);
		// Millenium is added because for negative numbers, integer division truncates upwards! (-8/10 = 0)
		return (((MILLENIUM + time) / DAY) - (1000 * 365));
	}
	
	public static long sortableTimeStringToMS(String string) throws DataFormatException {
		// Calendar calendar = new GregorianCalendar();
		try {
			int year = Integer.parseInt(string.substring(0, 4));
			int month = Integer.parseInt(string.substring(4, 6)) - 1;
			int day = Integer.parseInt(string.substring(6, 8));
			calendar.set(year, month, day);
			long time = calendar.getTimeInMillis();
			time += calendar.getTimeZone().getOffset(time);
			
			if (string.length() > 8) {
				int hour = Integer.parseInt(string.substring(8, 10));
				time += hour * 60 * 60 * 1000;
				if (string.length() > 8) {
					int minute = Integer.parseInt(string.substring(10, 12));
					time += minute * 60 * 1000;
					if (string.length() > 8) {
						int second = Integer.parseInt(string.substring(12, 14));
						time += second * 1000;
					}
				}
			}
			// Millenium is added because for negative numbers, integer division truncates upwards! (-8/10 = 0)
			return (time);
		} catch (Exception e) {
			throw new DataFormatException("Error parsing date: \"" + string + "\"");
		}
	}
	
	public static String replaceInternationalChars(String string) {
		char result[] = string.toCharArray();
		for (int i = 0; i < result.length; i++) {
			char ch = result[i];
			int charInt = (int) ch;
			if (charInt == 216)
				result[i] = 'O';
			else if (charInt == 248)
				result[i] = 'o';
			else if (charInt == 246)
				result[i] = 'o';
			else if (charInt == 244)
				result[i] = 'o';
			else if (charInt == 245)
				result[i] = 'o';
			else if (charInt == 242)
				result[i] = 'o';
			else if (charInt == 243)
				result[i] = 'o';
			else if (charInt == 237)
				result[i] = 'i';
			else if (charInt == 238)
				result[i] = 'i';
			else if (charInt == 239)
				result[i] = 'i';
			else if (charInt == 232)
				result[i] = 'e';
			else if (charInt == 233)
				result[i] = 'e';
			else if (charInt == 234)
				result[i] = 'e';
			else if (charInt == 235)
				result[i] = 'e';
			else if (charInt == 231)
				result[i] = 'c';
			else if (charInt == 224)
				result[i] = 'a';
			else if (charInt == 225)
				result[i] = 'a';
			else if (charInt == 226)
				result[i] = 'a';
			else if (charInt == 227)
				result[i] = 'a';
			else if (charInt == 228)
				result[i] = 'a';
			else if (charInt == 229)
				result[i] = 'a';
			else if (charInt == 252)
				result[i] = 'u';
			else if (charInt == 250)
				result[i] = 'u';
			else if (charInt == 253)
				result[i] = 'y';
			else if (charInt == 241)
				result[i] = 'n';
		}
		return new String(result);
	}
	
	public static int caseInsensitiveIndexOf(String value, List<String> list) {
		String queryLC = value.toLowerCase();
		for (int i = 0; i < list.size(); i++) {
			String string = list.get(i);
			if (string.toLowerCase().equals(queryLC))
				return i;
		}
		return -1;
	}
	
	public static int levenshteinDistance(String s, String t) {
		int d[][]; // matrix
		int n; // length of s
		int m; // length of t
		int i; // iterates through s
		int j; // iterates through t
		char s_i; // ith character of s
		char t_j; // jth character of t
		int cost; // cost
		n = s.length();
		m = t.length();
		if (n == 0) {
			return m;
		}
		if (m == 0) {
			return n;
		}
		d = new int[n + 1][m + 1];
		for (i = 0; i <= n; i++) {
			d[i][0] = i;
		}
		for (j = 0; j <= m; j++) {
			d[0][j] = j;
		}
		for (i = 1; i <= n; i++) {
			s_i = s.charAt(i - 1);
			for (j = 1; j <= m; j++) {
				t_j = t.charAt(j - 1);
				if (s_i == t_j) {
					cost = 0;
				} else {
					cost = 1;
				}
				d[i][j] = Math.min(d[i - 1][j] + 1, Math.min(d[i][j - 1] + 1, d[i - 1][j - 1] + cost));
			}
		}
		return d[n][m];
	}
	
	/**
	 * Get hex string interpretation of the 16-byte MD5 hash for an input string Author: Kristina
	 */
	public static String getMD5Digest(String str) {
		try {
			byte[] buffer = str.getBytes();
			byte[] result = null;
			StringBuffer buf = null;
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			// allocate room for the hash
			result = new byte[md5.getDigestLength()];
			// calculate hash
			md5.reset();
			md5.update(buffer);
			
			result = md5.digest();
			// System.out.println(result);
			// create hex string from the 16-byte hash
			buf = new StringBuffer(result.length * 2);
			for (int i = 0; i < result.length; i++) {
				int intVal = result[i] & 0xff;
				if (intVal < 0x10) {
					buf.append("0");
				}
				buf.append(Integer.toHexString(intVal).toUpperCase());
			}
			return buf.toString();
		} catch (NoSuchAlgorithmException e) {
			System.err.println("Exception caught: " + e);
			e.printStackTrace();
			
		}
		return null;
	}
	
	/**
	 * Get hex string interpretation of the SHA-256 hash for an input string Author: Kristina
	 */
	public static String getSHA256Digest(String str) {
		try {
			byte[] buffer = str.getBytes();
			byte[] result = null;
			StringBuffer buf = null;
			MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
			// allocate room for the hash
			result = new byte[sha256.getDigestLength()];
			// calculate hash
			sha256.reset();
			sha256.update(buffer);
			
			result = sha256.digest();
			// System.out.println(result);
			// create hex string from the 16-byte hash
			buf = new StringBuffer(result.length * 2);
			for (int i = 0; i < result.length; i++) {
				int intVal = result[i] & 0xff;
				if (intVal < 0x10) {
					buf.append("0");
				}
				buf.append(Integer.toHexString(intVal).toUpperCase());
			}
			return buf.toString();
		} catch (NoSuchAlgorithmException e) {
			System.err.println("Exception caught: " + e);
			e.printStackTrace();
			
		}
		return null;
	}
	
	public static String wordWrap(String text, int lineLength) {
		text = text.trim();
		if (text.length() < lineLength)
			return text;
		if (text.substring(0, lineLength).contains("\n"))
			return text.substring(0, text.indexOf("\n")).trim() + "\n\n" + wordWrap(text.substring(text.indexOf("\n") + 1), lineLength);
		int place = Math.max(Math.max(text.lastIndexOf(" ", lineLength), text.lastIndexOf("\t", lineLength)), text.lastIndexOf("-", lineLength));
		if (place > 0) {
			return text.substring(0, place).trim() + "\n" + wordWrap(text.substring(place), lineLength);			
		}
		else {
			return text;
		}
	}
	
	public static boolean isDate(String string) {
		if (string.length() == 10) {
			if ((string.charAt(4) == '-' || string.charAt(4) == '/') || (string.charAt(4) == string.charAt(7)))
				try {
					int year = Integer.parseInt(string.substring(0, 4));
					if (year < 1700 || year > 2200)
						return false;
					int month = Integer.parseInt(string.substring(5, 7));
					if (month < 1 || month > 12)
						return false;
					int day = Integer.parseInt(string.substring(8, 10));
					if (day < 1 || day > 31)
						return false;
					return true;
				} catch (Exception e) {
					return false;
				}
		} else if (string.length() == 8) {
			if ((string.charAt(2) == '-' || string.charAt(5) == '/') || (string.charAt(2) == string.charAt(5)))
				try {
					Integer.parseInt(string.substring(6, 8));
					int month = Integer.parseInt(string.substring(0, 2));
					if (month < 1 || month > 12)
						return false;
					int day = Integer.parseInt(string.substring(3, 5));
					if (day < 1 || day > 31)
						return false;
					return true;
				} catch (Exception e) {
					return false;
				}
			
		}
		return false;
	}
	
}
