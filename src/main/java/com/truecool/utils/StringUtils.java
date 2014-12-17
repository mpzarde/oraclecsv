package com.truecool.utils;


import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: mpzarde
 * Date: Jan 30, 2006
 * Time: 9:47:54 PM
 */
public class StringUtils {
  public static boolean isValidInteger(String string) {
    boolean returnValue = true;

    try {
      int somenumber = Integer.parseInt(string);
    } catch (NumberFormatException e) {
      returnValue = false;
    }

    return returnValue;
  }

  public static boolean isValidDouble(String string) {
    boolean returnValue = true;

    try {
      double somenumber = Double.parseDouble(string);
    } catch (NumberFormatException e) {
      returnValue = false;
    }

    return returnValue;
  }

  public static boolean isValidClob(String string) {
    boolean returnValue = false;

    if (string != null && string.startsWith("CLOB=")) {
      returnValue = true;
    }

    return returnValue;
  }

  public static boolean isValidDate(String string) {
    boolean returnValue = true;

    try {
      SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
      Date date = formatter.parse(string, new ParsePosition(0));

      if (date != null) {
        returnValue = true;
      } else {
        returnValue = false;
      }
    } catch (Exception e) {
      returnValue = false;
    }

    return returnValue;
  }

  public static Date getValidDate(String string, String format) {
    Date date = null;

    SimpleDateFormat formatter = new SimpleDateFormat(format);
    date = formatter.parse(string, new ParsePosition(0));

    if (date == null) {
      formatter = new SimpleDateFormat("mm/DD/yyyy");
      date = formatter.parse(string, new ParsePosition(0));
    }
    return date;
  }

  public static String replace(String originalString, String oldSubString, String newSubString, boolean all) {

    if (originalString == null || oldSubString == null || oldSubString.length() == 0 || newSubString == null) {
      throw new IllegalArgumentException("null or empty String");
    }

    StringBuffer result = new StringBuffer();
    int oldpos = 0;
    int nextpos;

    do {
      nextpos = originalString.indexOf(oldSubString, oldpos);

      if (nextpos < 0) {
        break;
      }

      result.append(originalString.substring(oldpos, nextpos));
      result.append(newSubString);
      nextpos += oldSubString.length();
      oldpos = nextpos;
    } while (all);

    if (oldpos == 0) {
      return originalString;
    } else {
      result.append(originalString.substring(oldpos));
      return new String(result);
    }
  }


}
