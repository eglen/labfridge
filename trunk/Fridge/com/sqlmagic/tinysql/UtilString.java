/*
 * This class provides string manipulation methods.
 *
 * $Author: davis $
 * $Date: 2004/12/18 21:23:31 $
 * $Revision: 1.1 $
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 *
 * Revision History:
 *
 * Written by Davis Swan in February, 2004.
 */
package com.sqlmagic.tinysql;

import java.text.*;
import java.util.*;
import java.lang.*;
import java.sql.Types;

public class UtilString
{
/*
 * Is this a quoted string?
 */
   public static boolean isQuotedString(String inputString)
   {
      String trimString;
      int trimLength;
      if ( inputString == (String)null ) return false;
      trimString = inputString.trim();
      trimLength = trimString.length();
      if ( trimString.length() == 0 ) return false;
      if ( ( trimString.charAt(0) == '\'' &
             trimString.charAt(trimLength - 1) == '\'' ) |
           ( trimString.charAt(0) == '"' & 
             trimString.charAt(trimLength - 1) == '"' ) )
      {
         return true;
      }
      return false;
   }
/*
 * Remove enclosing quotes from a string.
 */
   public static String removeQuotes(String inputString)
   {
      String trimString;
      int trimLength;
      if ( inputString == (String)null ) return inputString;
      trimString = inputString.trim();
      trimLength = trimString.length();
      if ( trimString.length() == 0 ) return inputString;
      if ( ( trimString.charAt(0) == '\'' &
             trimString.charAt(trimLength - 1) == '\'' ) |
           ( trimString.charAt(0) == '"' & 
             trimString.charAt(trimLength - 1) == '"' ) )
      {
         return trimString.substring(1,trimString.length() - 1);
      }
      return inputString;
   }
/*
 * Convert a string to a double or return a default value.
 */
   public static double doubleValue(String inputString)
   {
      return doubleValue(inputString,Double.MIN_VALUE);
   }
   public static double doubleValue(String inputString,double defaultValue )
   {
      try
      {
         return Double.parseDouble(inputString);
      } catch (Exception e) {
         return defaultValue;
      }
   }
/*
 * The following method replaces all occurrences of oldString with newString
 * in the inputString.  This function can be replaced with the native
 * String method replaceAll in JDK 1.4 and above but is provide to support
 * earlier versions of the JRE.
 */
   public static String replaceAll(String inputString,String oldString,
      String newString)
   {
      StringBuffer outputString = new StringBuffer(100);
      int startIndex=0,nextIndex;
      while ( inputString.substring(startIndex).indexOf(oldString) > -1 )
      {
         nextIndex = startIndex + inputString.substring(startIndex).indexOf(oldString);
         if ( nextIndex > startIndex )
         { 
            outputString.append(inputString.substring(startIndex,nextIndex));
         }
         outputString.append(newString);
         startIndex = nextIndex + oldString.length();
      }
      if ( startIndex <= inputString.length() - 1 )
      {
         outputString.append(inputString.substring(startIndex));
      }
      return outputString.toString();
   }
/*
 * Convert a string to an int or return a default value.
 */
   public static int intValue(String inputString,int defaultValue )
   {
      try
      {
         return Integer.parseInt(inputString);
      } catch (Exception e) {
         return defaultValue;
      }
   }
/*
 * This method formats an action Hashtable for display.
 */
   public static String actionToString(Hashtable displayAction)
   {
      StringBuffer displayBuffer = new StringBuffer();
      String displayType,tableName,columnContext;
      tinySQLWhere displayWhere;
      tsColumn createColumn;
      boolean groupBy=false,orderBy=false;
      int i;
      Vector displayTables,displayColumns,columnDefs,displayValues,
      displayContext;
      displayType = (String)displayAction.get("TYPE");
      displayBuffer.append(displayType + " ");
      displayWhere = (tinySQLWhere)null;
      displayContext = (Vector)null;
      displayColumns = (Vector)null;
      if ( displayType.equals("SELECT") )
      {
         displayTables = (Vector)displayAction.get("TABLES");
         displayColumns = (Vector)displayAction.get("COLUMNS");
         displayContext = (Vector)displayAction.get("CONTEXT");
         displayWhere = (tinySQLWhere)displayAction.get("WHERE");
         for ( i = 0; i < displayColumns.size(); i++ )
         {
            columnContext = (String)displayContext.elementAt(i);
            if ( columnContext.equals("GROUP") )
            {
               groupBy = true;
               continue;
            } else if ( columnContext.equals("ORDER") ) {
               orderBy = true;
               continue;
            }
            if ( i > 0 ) displayBuffer.append(",");
            displayBuffer.append((String)displayColumns.elementAt(i));
         }
         displayBuffer.append(" FROM " );
         for ( i = 0; i < displayTables.size(); i++ )
         {
            if ( i > 0 ) displayBuffer.append(",");
            displayBuffer.append((String)displayTables.elementAt(i));
         }
      } else if ( displayType.equals("DROP_TABLE") ) {
         tableName = (String)displayAction.get("TABLE");
         displayBuffer.append(tableName);
      } else if ( displayType.equals("CREATE_TABLE") ) {
         tableName = (String)displayAction.get("TABLE");
         displayBuffer.append(tableName + " (");
         columnDefs = (Vector)displayAction.get("COLUMN_DEF");
         for ( i = 0; i < columnDefs.size(); i++ )
         {
            if ( i > 0 ) displayBuffer.append(",");
            createColumn = (tsColumn)columnDefs.elementAt(i);
            displayBuffer.append(createColumn.name + " " + createColumn.type
            + "( " + createColumn.size + "," + createColumn.decimalPlaces
            + ")");
         }
         displayBuffer.append(")");
      } else if ( displayType.equals("INSERT") ) {
         tableName = (String)displayAction.get("TABLE");
         displayBuffer.append("INTO " + tableName + "(");
         displayColumns = (Vector)displayAction.get("COLUMNS");
         for ( i = 0; i < displayColumns.size(); i++ )
         {
            if ( i > 0 ) displayBuffer.append(",");
            displayBuffer.append((String)displayColumns.elementAt(i));
         }
         displayBuffer.append(") VALUES (");
         displayValues = (Vector)displayAction.get("VALUES");
         for ( i = 0; i < displayValues.size(); i++ )
         {
            if ( i > 0 ) displayBuffer.append(",");
            displayBuffer.append((String)displayValues.elementAt(i));
         }
         displayBuffer.append(")");
      } else if ( displayType.equals("UPDATE") ) {
         tableName = (String)displayAction.get("TABLE");
         displayBuffer.append(tableName + " SET ");
         displayColumns = (Vector)displayAction.get("COLUMNS");
         displayValues = (Vector)displayAction.get("VALUES");
         displayWhere = (tinySQLWhere)displayAction.get("WHERE");
         for ( i = 0; i < displayColumns.size(); i++ )
         {
            if ( i > 0 ) displayBuffer.append(",");
            displayBuffer.append((String)displayColumns.elementAt(i)
            + "=" + (String)displayValues.elementAt(i));
         }
      } else if ( displayType.equals("DELETE") ) {
         tableName = (String)displayAction.get("TABLE");
         displayBuffer.append(" FROM " + tableName);
         displayWhere = (tinySQLWhere)displayAction.get("WHERE");
      }
      if ( displayWhere != (tinySQLWhere)null )
      {
         displayBuffer.append(displayWhere.toString());
      }
      if ( groupBy ) 
      {
         displayBuffer.append(" GROUP BY ");
         for ( i = 0; i < displayColumns.size(); i++ )
         {
            columnContext = (String)displayContext.elementAt(i);
            if ( !columnContext.equals("GROUP") ) continue;
            if ( !displayBuffer.toString().endsWith(" GROUP BY ") )
               displayBuffer.append(",");
            displayBuffer.append((String)displayColumns.elementAt(i));
         }
      }
      if ( orderBy ) 
      {
         displayBuffer.append(" ORDER BY ");
         for ( i = 0; i < displayColumns.size(); i++ )
         {
            columnContext = (String)displayContext.elementAt(i);
            if ( !columnContext.equals("ORDER") ) continue;
            if ( !displayBuffer.toString().endsWith(" ORDER BY ") )
               displayBuffer.append(",");
            displayBuffer.append((String)displayColumns.elementAt(i));
         }
      }
      return displayBuffer.toString();
   }
/*
 * Find the input table alias in the list provided and return the table name.
 */
   public static String findTableForAlias(String inputAlias,Vector tableList)
                              throws tinySQLException
   {
      int i,aliasAt;
      String tableAndAlias;
      tableAndAlias = findTableAlias(inputAlias,tableList);
      aliasAt = tableAndAlias.indexOf("->");
      return tableAndAlias.substring(0,aliasAt);
   }
/*
 * Find the input table alias in the list provided and return the table name
 * and alias in the form tableName=tableAlias.
 */
   public static String findTableAlias(String inputAlias,Vector tableList)
                              throws tinySQLException
   {
      int i,aliasAt;
      String tableAndAlias,tableName,tableAlias;
      for ( i = 0; i < tableList.size(); i++ )
      {
         tableAndAlias = (String)tableList.elementAt(i);
         aliasAt = tableAndAlias.indexOf("->");
         tableName = tableAndAlias.substring(0,aliasAt);
         tableAlias = tableAndAlias.substring(aliasAt + 2);
         if ( inputAlias.equals(tableAlias) )
         {
            return tableAndAlias;
         }
      }
      throw new tinySQLException("Unable to identify table alias "
      + inputAlias);
   }
/*
 * Determine a type for the input string. 
 */
   public static int getValueType (String inputValue)
   {
      double doubleValue,floorValue;
      long intValue;
      if ( inputValue.startsWith("\"") |
           inputValue.startsWith("'") )
         return Types.CHAR;
      try 
      {
         doubleValue = Double.parseDouble(inputValue.trim());
         floorValue = Math.floor(doubleValue);
         if ( floorValue == doubleValue ) 
            return Types.INTEGER;
         else
            return Types.FLOAT;
      } catch (Exception e) {
         return Types.CHAR;
      }
   }
}
