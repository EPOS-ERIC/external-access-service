/*******************************************************************************
 * Copyright 2021 EPOS ERIC
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/
package org.epos.api.utility;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fatboyindustrial.gsonjavatime.Converters;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class Utils {

	private static final  Logger LOGGER = LoggerFactory.getLogger(Utils.class);

	public static final String EPOSINTERNALFORMAT = "yyyy-MM-ddThh:mm:ssZ";

	public static Gson gson = Converters.registerAll(new GsonBuilder()).registerTypeHierarchyAdapter(Collection.class, new CollectionAdapter()).create();

	public static <T> List<T> union(List<T> list1, List<T> list2) {
		Set<T> set = new HashSet<T>();

		set.addAll(list1);
		set.addAll(list2);

		return new ArrayList<T>(set);
	}

	public static <T> List<T> intersection(List<T> list1, List<T> list2) {
		List<T> list = new ArrayList<T>();

		for (T t : list1) {
			if(list2.contains(t)) {
				list.add(t);
			}
		}

		return list;
	}

	public static <T> T mergeObjects(T first, T second){
		Class<?> clas = first.getClass();
		Field[] fields = clas.getDeclaredFields();
		Object result = null;
		try {
			result = clas.getDeclaredConstructor().newInstance();
			for (Field field : fields) {
				field.setAccessible(true);
				Object value1 = field.get(first);
				Object value2 = field.get(second);
				Object value = (value1 != null) ? value1 : value2;
				field.set(result, value);
			}
		} catch (Exception e) {
			LOGGER.error(e.toString());
		}
		return (T) result;
	}

	public static boolean stringContainsItemFromList(String inputStr, String[] items) {
		return Arrays.stream(items).anyMatch(inputStr::contains);
	}

	public static void stripEmpty(JsonNode node) {
		Iterator<JsonNode> it = node.iterator();
		while (it.hasNext()) {
			JsonNode child = it.next();
			if (child.isObject() && child.isEmpty(null))
				it.remove();
			else
				stripEmpty(child);
		}
	}

	static class CollectionAdapter implements JsonSerializer<Collection<?>> {
		@Override
		public JsonElement serialize(Collection<?> src, Type typeOfSrc, JsonSerializationContext context) {
			if (src == null || src.isEmpty())
				return null;

			JsonArray array = new JsonArray();

			for (Object child : src) {
				JsonElement element = context.serialize(child);
				array.add(element);
			}

			return array;
		}

	}

	public static Boolean checkStringPattern(String inputString, String inputPattern) {
		return Pattern.matches(inputPattern, inputString);
	}
	
	public static Boolean checkStringPatternSingleQuotes(String inputPattern) {
		return inputPattern.equals("^'[^']*'$");
	}

	public static String convertISOPatternToJavaFormatPattern(String pattern) {
		if(pattern.contains("T") && !pattern.contains("'T'")) pattern = pattern.replace("T", "'T'");
		if(pattern.contains("Z") && !pattern.contains("'Z'")) pattern = pattern.replace("Z", "'Z'");
		pattern = pattern.replace("YYYY", "yyyy").replace("DD", "dd").replace("hh", "HH");
		return pattern;
	}

	public static String convertDateUsingPattern(String dateString, String inputFormat, String outputFormat) throws ParseException {
		if(isValidFormat(convertISOPatternToJavaFormatPattern(outputFormat), dateString)) return dateString;
		if(inputFormat==null) inputFormat=EPOSINTERNALFORMAT;
        if(dateString == null) throw new NullPointerException();

		String dateConverted = null;

		if(outputFormat.equals("YYYY.yyy")) {
			dateConverted = Float.toString(fromDateToDecimalYear(new SimpleDateFormat("yyyy-MM-dd").parse(dateString)));
		} else if(inputFormat.equals("YYYY.yyy")) {
			dateConverted = fromDecimalYearToDate(Float.parseFloat(dateString));
		}
		else {
			if(!isValidFormat(convertISOPatternToJavaFormatPattern(inputFormat), dateString)) {
				inputFormat = EPOSINTERNALFORMAT;
			}

			inputFormat = convertISOPatternToJavaFormatPattern(inputFormat);
			outputFormat = convertISOPatternToJavaFormatPattern(outputFormat);
			DateTimeFormatter format = org.joda.time.format.DateTimeFormat.forPattern(inputFormat);
			LocalDateTime lDate = null;
			LOGGER.debug( "convert date: "+ dateString+ " "+inputFormat);

			DateTime dt = new DateTime(dateString);
			LOGGER.debug( "convert date, as iso: "+ dt.toDateTimeISO());
			try {
				lDate = org.joda.time.LocalDateTime.parse(dt.toString(), format);
			}catch(Exception e) {
				try {
					lDate = org.joda.time.LocalDateTime.parse(dateString, format);
				}catch(Exception e1) {
					lDate = dt.toLocalDateTime();
				}
			}

			dateConverted = lDate.toString();
			dateConverted = DateTime.parse(dateConverted).toString(outputFormat);

		}
		return dateConverted;

	}

	private static float fromDateToDecimalYear(Date myDate) throws ParseException {
		DateTime dt = new DateTime(myDate);
		Calendar c = new GregorianCalendar();
		c.setTime(myDate);
		int daysInYear = c.getActualMaximum(Calendar.DAY_OF_YEAR);
		int dayNumber = dt.getDayOfYear();
		float day = ((float)(dayNumber-1)/daysInYear) + dt.getYear();
		return Float.parseFloat(String.format("%.3f", day).replace(',', '.'));
	}

	private static String fromDecimalYearToDate(float date) throws ParseException {
		int year = (int) date;

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
		Calendar c = new GregorianCalendar();
		c.setTime(new Date(year));

		int daysInYear = c.getActualMaximum(Calendar.DAY_OF_YEAR);

		float day = (date - year)*daysInYear;
		c.add(Calendar.DATE, (int)day);
		c.set(Calendar.YEAR, year);

		return sdf.format(c.getTime());
	}

	public static boolean isValidFormat(String format, String value) {
		Date date = null;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			date = sdf.parse(value);
			if (!value.equals(sdf.format(date))) {
				date = null;
			}
		} catch (ParseException ex) {
			LOGGER.warn("Error parsing date {}", ex.getLocalizedMessage());
		}
		return date != null;
	}

}
