/*
 * tsColumn.java - Column Object for tinySQL.
 * 
 * Copyright 1996, Brian C. Jepson
 *                 (bjepson@ids.net)
 * $Author: davis $
 * $Date: 2004/12/18 21:25:35 $
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
 */

package com.sqlmagic.tinysql;

import java.util.*;
import java.lang.*;
import java.io.*;
import java.text.*;
import java.sql.Types;

/*
 * Object to hold column metadata and value
 * Example for a column_def entity:
 * phone  CHAR(30)  DEFAULT '-'  NOT NULL
 *
 * @author Thomas Morgner <mgs@sherito.org> type is now integer 
 * and contains one of the java.sql.Types Values
 */
class tsColumn 
{
   public String name = null;      // the column's name
   public String alias = null;      // the column's definition
   public int type = -1;      // the column's type
                                   // dBase types:
                                   // 'C' Char (max 254 bytes)
                                   // 'N' '-.0123456789' (max 19 bytes)
                                   // 'L' 'YyNnTtFf?' (1 byte)
                                   // 'M' 10 digit .DBT block number
                                   // 'D' 8 digit YYYYMMDD
   public int    size = 0;         // the column's size
   public int decimalPlaces = 0;   // decimal places in numeric column
   public String defaultVal = null;// not yet supported
   public boolean notNull = false; // not yet supported, true: NOT NULL
   public int position = 0;        // internal use
   public String table = null; // the table which "owns" the column
   public String newLine = System.getProperty("line.separator");
   String functionName = (String)null;  // Function name
   String functionArgString = (String)null;  // Function arguments
   Vector functionArgs = (Vector)null; // Function arguments as columns
   boolean isNotNull = false;
   String stringValue = (String)null;
   int intValue = Integer.MIN_VALUE;
   float floatValue = Float.MIN_VALUE;
   SimpleDateFormat fmtyyyyMMdd = new SimpleDateFormat("yyyyMMdd");
   Calendar today = Calendar.getInstance();
   boolean debug = false;
   boolean isConstant = false;
   boolean groupedColumn = false;
/*
 * The constructor creates a column object using recursion if this is a 
 * function.
 */
   tsColumn (String s) throws tinySQLException
   {
      this(s,(Hashtable)null);
   }
   tsColumn (String s, Hashtable tableDefs) throws tinySQLException
   {
      FieldTokenizer ft,ftArgs;
      int i,j,numericType,nameLength,dotAt,argIndex;
      String upperName,tableName,checkName,nextArg;
      tinySQLTable jtbl,foundTable;
      tsColumn tcol;
      Vector t;
      Enumeration col_keys;
      name = s;
      nameLength = name.length();
      ft = new FieldTokenizer(name,'(',false);
      if ( ft.countFields() == 2 ) 
      {
/*
 *       This is a function rather than a simple column or constant
 */
         functionName = ft.getField(0).toUpperCase();
         if ( functionName.equals("COUNT") )
         {
            type = Types.INTEGER;
            size = 10;
            intValue = 0;
            groupedColumn = true;
         } else if ( functionName.equals("SUM") ) {
            type = Types.FLOAT;
            size = 10;
            groupedColumn = true;
         } else if ( functionName.equals("CONCAT") |
                     functionName.equals("UPPER") |
                     functionName.equals("SUBSTR") ) {
            type = Types.CHAR;
         }
         functionArgString = ft.getField(1);
         ftArgs = new FieldTokenizer(functionArgString,',',false);
         functionArgs = new Vector();
         argIndex = 0;
         while ( ftArgs.hasMoreFields() )
         {
            nextArg = ftArgs.nextField();
            tcol = new tsColumn(nextArg,tableDefs);
            if ( tcol.isGroupedColumn() ) groupedColumn = true;
/*
 *          MAX and MIN functions can be either FLOAT or CHAR types
 *          depending upon the type of the argument.
 */
            if ( functionName.equals("MAX") | functionName.equals("MIN") ) 
            {
               if ( argIndex > 0 ) 
                  throw new tinySQLException("Function can only have 1 argument");
               groupedColumn = true;
               type = tcol.type;
               size = tcol.size;
            } else if ( functionName.equals("CONCAT") ) {
               type = Types.CHAR; 
               size += tcol.size;
            } else if ( functionName.equals("UPPER") ) {
               type = Types.CHAR; 
               size = tcol.size;
            } else if ( functionName.equals("SUBSTR") ) {
               type = Types.CHAR;
               if ( argIndex == 0 & tcol.type != Types.CHAR )
               {
                  throw new tinySQLException("SUBSTR first argument must be character");
               } else if ( argIndex == 1 ) {
                  if ( tcol.type != Types.INTEGER | tcol.intValue < 1 )
                  throw new tinySQLException("SUBSTR second argument "
                  + tcol.getString() + " must be integer > 0");
               } else if ( argIndex == 2 ) {
                  if ( tcol.type != Types.INTEGER | tcol.intValue < 1)
                  throw new tinySQLException("SUBSTR third argument "
                  + tcol.getString() + " must be integer > 0");
                  size = tcol.intValue;
               }  
            }
            argIndex++;
            functionArgs.addElement(tcol);
         }
      } else {
/*
 *       Check for SYSDATE
 */
         if ( name.toUpperCase().equals("SYSDATE") )
         {
            isConstant = true;
            type = Types.DATE;
            isNotNull = true;
            stringValue = fmtyyyyMMdd.format(today.getTime());
/*
 *          Check for a quoted string
 */
         } else if ( UtilString.isQuotedString(name) ) {
            isConstant = true;
            type = Types.CHAR;
            stringValue = UtilString.removeQuotes(name);
            if ( stringValue != (String)null )
            {
               size = stringValue.length(); 
               isNotNull = true;
            }
         } else {
/*
 *          Check for a numeric constant
 */
            numericType = UtilString.getValueType(name);
            if ( numericType == Types.INTEGER )
            {
               intValue = Integer.valueOf(name).intValue();
               size = 10;
               type = numericType;
               isConstant = true;
               isNotNull = true;
	    } else if ( numericType == Types.FLOAT ) {
               floatValue = Float.valueOf(name).floatValue();
               size = 10;
               type = numericType;
               isConstant = true;
               isNotNull = true;
            } else {
/*
 *             This should be a column name. 
 */
               foundTable = (tinySQLTable)null;
               upperName = name.toUpperCase();
               if ( debug )
                  System.out.println("Trying to find table for " + upperName);
               dotAt = upperName.indexOf(".");
               if ( dotAt > -1 ) 
               {
                  tableName = upperName.substring(0,dotAt);
                  if ( tableDefs != (Hashtable)null &
                       tableName.indexOf("->") < 0 )
                  {
                     t = (Vector)tableDefs.get("TABLE_SELECT_ORDER");
                     tableName = UtilString.findTableAlias(tableName,t);
                  }
                  table = tableName;
                  upperName = upperName.substring(dotAt + 1);
                  foundTable = (tinySQLTable)tableDefs.get(tableName);
               } else if ( tableDefs != (Hashtable)null ) {
/*
 *                Use an enumeration to go through all of the tables to find
 *                this column.
 */
                  t = (Vector)tableDefs.get("TABLE_SELECT_ORDER");
                  for ( j = 0; j < t.size(); j++ )
                  {
                     tableName = (String)t.elementAt(j);
                     jtbl = (tinySQLTable)tableDefs.get(tableName);
                     col_keys = jtbl.column_info.keys();
/*
 *                   Check all columns.
 */
                     while (col_keys.hasMoreElements()) 
                     {
                        checkName = (String)col_keys.nextElement();
                        if ( checkName.equals(upperName) |
                             upperName.equals("*") )
                        {
                           upperName = checkName;
                           foundTable = jtbl;
                           break;
                        }
                     }
                     if ( foundTable != (tinySQLTable)null ) break;
                  }
               } else {
                  if ( debug ) System.out.println("No table definitions.");
               }
               if ( foundTable != (tinySQLTable)null ) 
               {
                  name = foundTable.table + "->" + foundTable.tableAlias
                     + "." + upperName;
                  type = foundTable.ColType(upperName);
                  size = foundTable.ColSize(upperName);
                  decimalPlaces = foundTable.ColDec(upperName);
                  table = foundTable.table;
               }
            }
         }
      }
   }
   public void update(String inputColumnName,String inputColumnValue)
      throws tinySQLException
   {
      int i,startAt,charCount;
      tsColumn argColumn;
      StringBuffer concatBuffer;
      if ( isConstant ) return;
      if ( functionName == (String)null )
      {
         if ( inputColumnName.equals(name) ) 
         {
            if ( debug ) System.out.println("Update " + name + " with column "
            + inputColumnName + "=" + inputColumnValue); 
/*
 *          If this is a simple column value, reset to null before
 *          trying to interpret the inputColumnValue.
 */
            isNotNull = false;
            stringValue = (String)null;
            intValue = Integer.MIN_VALUE;
            floatValue = Float.MIN_VALUE;
            if ( inputColumnValue != (String)null )
               if ( inputColumnValue.trim().length() > 0 ) isNotNull = true;
            if ( type == Types.CHAR | type == Types.DATE )
            {
               stringValue = inputColumnValue;
            } else if ( type == Types.INTEGER & isNotNull ) {
               try
               {
                  intValue = Integer.parseInt(inputColumnValue.trim()); 
               } catch (Exception ex) {
                  throw new tinySQLException(inputColumnValue + " is not an integer.");
               }
            } else if ( type == Types.FLOAT & isNotNull ) {
               try
               {
                  floatValue = Float.valueOf(inputColumnValue.trim()).floatValue(); 
               } catch (Exception ex) {
                  throw new tinySQLException(inputColumnValue + " is not a Float.");
               }
            }
         }
      } else if ( functionName.equals("CONCAT") ) {
         concatBuffer = new StringBuffer();
         for ( i = 0; i < functionArgs.size(); i++ )
         {
            argColumn = (tsColumn)functionArgs.elementAt(i);
            argColumn.update(inputColumnName,inputColumnValue);
            if ( argColumn.isNotNull )
            {
               concatBuffer.append(argColumn.getString());
               isNotNull = true;
            }
         }
         stringValue = concatBuffer.toString();
      } else if ( functionName.equals("UPPER") ) {
         argColumn = (tsColumn)functionArgs.elementAt(0);
         argColumn.update(inputColumnName,inputColumnValue);
         if ( argColumn.isNotNull )
         {
            stringValue = argColumn.getString().toUpperCase();
            isNotNull = true;
         } 
      } else if ( functionName.equals("SUBSTR") ) {
         if ( functionArgs.size() != 3 ) 
            throw new tinySQLException("Wrong number of arguments for SUBSTR");
         argColumn = (tsColumn)functionArgs.elementAt(1);
         startAt = argColumn.intValue;
         argColumn = (tsColumn)functionArgs.elementAt(2);
         charCount = argColumn.intValue;
         argColumn = (tsColumn)functionArgs.elementAt(0);
         argColumn.update(inputColumnName,inputColumnValue);
         if ( argColumn.isNotNull )
         {
            stringValue = argColumn.stringValue;
            if ( startAt < stringValue.length() - 1 & charCount > 0 )
            {
               stringValue = stringValue.substring(startAt - 1 ,startAt + charCount - 1);
               isNotNull = true;
            } else {
               stringValue = (String)null;
            }
         }
      } else if ( functionName.equals("COUNT") ) {
         argColumn = (tsColumn)functionArgs.elementAt(0);
         if ( argColumn.name.equals(inputColumnName) & argColumn.isNotNull )
         {
            isNotNull = true;
            if ( intValue == Integer.MIN_VALUE ) 
            {
               intValue = 0;
            } else {
               intValue = intValue + 1;
            }
         }
      } else if ( functionName.equals("SUM") ) {
         argColumn = (tsColumn)functionArgs.elementAt(0);
         argColumn.update(inputColumnName,inputColumnValue);
         if ( argColumn.type == Types.CHAR | argColumn.type == Types.DATE )
            throw new tinySQLException(argColumn.name + " is not numeric!");
         if ( argColumn.name.equals(inputColumnName) & argColumn.isNotNull )
         {
            isNotNull = true;
            if ( floatValue == Float.MIN_VALUE ) 
            {
               floatValue = (float)0.0;
            } else {
               if ( argColumn.type == Types.INTEGER )
                  floatValue += new Integer(argColumn.intValue).floatValue();
               else
                  floatValue += argColumn.floatValue;
            }
         }
      } else if ( functionName.equals("MAX") | functionName.equals("MIN") ) {
         argColumn = (tsColumn)functionArgs.elementAt(0);
         argColumn.update(inputColumnName,inputColumnValue);
         if ( argColumn.name.equals(inputColumnName) & argColumn.isNotNull )
         {
            isNotNull = true;
            if ( argColumn.type == Types.CHAR | argColumn.type == Types.DATE )
            {
               if ( stringValue == null )
               {
                  stringValue = argColumn.stringValue;
               } else {
/* 
 *                Update the max and min based upon string comparisions.
 */
                  if ( functionName.equals("MAX") &
                     ( argColumn.stringValue.compareTo(stringValue) > 0 ) )
                  {
                     stringValue = argColumn.stringValue;
                  } else if ( functionName.equals("MIN") &
                     ( argColumn.stringValue.compareTo(stringValue) < 0 ) ) {
                     stringValue = argColumn.stringValue;
                  }
               }
            } else if ( argColumn.type == Types.INTEGER ) {
/*
 *             Update max and min based upon numeric values.
 */
               if ( intValue == Integer.MIN_VALUE )
               {
                  intValue = argColumn.intValue;
               } else {
                  if ( functionName.equals("MIN") &
                     argColumn.intValue < intValue )
                     intValue = argColumn.intValue;
                  else if ( functionName.equals("MAX") &
                     argColumn.intValue > intValue )
                     intValue = argColumn.intValue;
               }
            } else if ( argColumn.type == Types.FLOAT ) {
               if ( floatValue == Float.MIN_VALUE ) 
               {
                  floatValue = argColumn.floatValue;
               } else {
                  if ( functionName.equals("MIN") &
                     argColumn.floatValue < floatValue )
                     floatValue = argColumn.floatValue;
                  else if ( functionName.equals("MAX") &
                     argColumn.floatValue > floatValue )
                     floatValue = argColumn.floatValue;
               }
            }
         }
      }
   }
   public boolean isGroupedColumn()
   {
      return groupedColumn;
   }
   public String getString()
   {
      if ( type == Types.CHAR | type == Types.DATE ) {
         return stringValue;
      } else if ( type == Types.INTEGER ) {
         if ( intValue == Integer.MIN_VALUE ) return (String)null;
         return Integer.toString(intValue);
      } else if ( type == Types.FLOAT ) {
         if ( floatValue == Float.MIN_VALUE ) return (String)null;
         return Float.toString(floatValue);
      }
      return (String)null;
   }
   public String toString()
   {
      int i;
      StringBuffer outputBuffer = new StringBuffer();
      if ( functionName == (String)null )
      {
         outputBuffer.append("-----------------------------------" + newLine
         + "Column Name: " + name + newLine 
         + "Table: " + table + newLine
         + "IsNotNull: " + isNotNull + newLine
         + "Type: " + type + newLine
         + "Size: " + size + newLine
         + "Value: " + getString());
      } else {
         outputBuffer.append("Function: " + functionName + newLine
         + "Type: " + type + newLine
         + "Size: " + size + newLine
         + "Value: " + getString());
         for ( i = 0; i < functionArgs.size(); i++ )
         {
            outputBuffer.append(newLine + "Argument " + i + " follows" + newLine
            + ((tsColumn)functionArgs.elementAt(i)).toString() + newLine);
         }
      }
      return outputBuffer.toString();
   }
}
