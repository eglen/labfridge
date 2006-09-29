/*
 * tinySQLWhere - Class to handle all Where clause processing.
 * 
 * $Author: davis $
 * $Date: 2004/12/18 21:24:13 $
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
 * Written by Davis Swan in May, 2004.
 */

package com.sqlmagic.tinysql;

import java.util.*;
import java.lang.*;
import java.io.*;
import java.sql.SQLException;
import java.sql.Types;

public class tinySQLWhere
{
   Vector whereClauseList;
   boolean debug=false;
/*
 * The constructor builds a Where clause object from the input string.
 */
   public tinySQLWhere(String whereString)
   {
      FieldTokenizer ft;
      Vector whereConditions;
      Object whereObj;
      String nextField,upperField,wherePhrase,comp,left,right,andOr;
      String whereCondition[];
      String[] comparisons = {"<=","=<",">=","=>","=","<>","!=",">","<",
      "LIKE","NOT LIKE"};
      boolean inBrackets=false;
      int i,foundKeyWord,foundComp,startAt,foundAnd,foundOr;
/*
 *    The whereClauseList is a Vector containing pointers to whereCondition
 *    Vectors or tinySQLWhere objects.
 */
      whereConditions = new Vector(); 
      whereClauseList = new Vector();
/*
 *    Identify any phrases that are contained within brackets.
 */
      ft = new FieldTokenizer(whereString,'(',true);
      while ( ft.hasMoreFields() )
      {
         nextField = ft.nextField();
         upperField = nextField.toUpperCase();
         if ( nextField.equals("(") )
         {
            whereObj = (Object)null;
            inBrackets = true;
         } else if ( nextField.equals(")") ) {
            inBrackets = false;
            whereObj = (Object)null;
         } else if ( inBrackets ) {
            whereObj = new tinySQLWhere(nextField);
            whereConditions.addElement(whereObj);
         } else {
/*
 *          Look for AND/OR keywords - if none are found process the
 *          entire string.
 */                     
            andOr = "AND";
            startAt = 0;
            whereConditions = new Vector();
            while ( startAt < upperField.length() )
            {
               foundAnd = upperField.indexOf(" AND",startAt);
/*
 *             Make sure this is not just part of a longer string.
 */
               if ( foundAnd > -1 & foundAnd < upperField.length() - 5 )
                  if ( upperField.charAt(foundAnd + 4) != ' ' )
                     foundAnd = -1; 
               foundOr = upperField.indexOf(" OR",startAt);
               if ( foundOr > -1 & foundOr < upperField.length() - 4 )
                  if ( upperField.charAt(foundOr + 3) != ' ' )
                     foundOr = -1; 
               foundKeyWord = upperField.length();
               if ( foundAnd > -1 ) foundKeyWord = foundAnd;
               if ( foundOr > -1 & foundOr < foundKeyWord )
                  foundKeyWord = foundOr;
               wherePhrase = nextField.substring(startAt,foundKeyWord);
               if ( debug ) 
                  System.out.println("Where phrase is " + wherePhrase);
               if ( foundKeyWord < upperField.length() - 4 )
                  andOr = upperField.substring(foundKeyWord+1,foundKeyWord+3);
               if ( andOr.equals("AN") ) andOr = "AND";
/*
 *             Build a whereCondition String array. The array elements are
 *             as follows:
 *             0 - command type     1 - left parameter  
 *             2 - comparison       3 - right parameter
 *             4 - left type        5 - left value
 *             6 - right type       7 - right value
 *             8 - status
 *
 *             Types can be CHAR, CHAR_CONSTANT, NUMBER, NUMBER_CONSTANT
 *             Status can be UNKNOWN,LEFT,RIGHT,BOTH,TRUE, or FALSE
 *             The status values indicate which parts of the where
 *             condition have been set.
 */
               whereCondition = new String[9];
               for ( i = 0; i < comparisons.length; i++ )
               {
                  comp = comparisons[i];
                  foundComp = wherePhrase.toUpperCase().indexOf(comp);
                  if ( foundComp > -1 )
                  {
                     whereCondition[0] = "JOIN";
                     left = wherePhrase.substring(0,foundComp).trim();
                     whereCondition[1] = left;
                     whereCondition[2] = comp;
                     right = wherePhrase.substring(foundComp + comp.length()).trim();
                     whereCondition[3] = right;
                     whereCondition[8] = "UNKNOWN";
                     break;
                  }
               }
               whereConditions.addElement(whereCondition);
/*
 *             If this condition and the previous one are joined by an
 *             AND keyword, add the condition to the existing Vector.
 *             For an OR keyword, create a new entry in the whereClauseList.
 */
               if ( andOr.equals("OR") )
               {
                  whereClauseList.addElement(whereConditions);
                  whereConditions = new Vector();
               }
               startAt = foundKeyWord + andOr.length() + 2;
            }
         }
      }
/*
 *    Add the last where condition to the list.
 */
      if ( whereConditions.size() > 0 ) 
         whereClauseList.addElement(whereConditions);
      if ( debug ) System.out.println("Where clause is \n" + toString());
   }
/*
 * This method adds a table name prefix to all columns referred to in 
 * the where clauses that don't have a table prefix yet.  It also sets the type
 * of the expression.  Input is a Hashtable of table objects.
 */
   public void setColumnTypes(tinySQLTable inputTable ) throws tinySQLException
   {
      Hashtable ht = new Hashtable();
      Vector tableList = new Vector();
      tableList.addElement(inputTable.table);
      ht.put(inputTable.table,inputTable);
      ht.put("TABLE_SELECT_ORDER",tableList);
      setColumnTypes(ht);
   }
   public void setColumnTypes(Hashtable tables) throws tinySQLException
   {
      int i,j,leftType,rightType,dotAt;
      tinySQLTable leftTable,rightTable;
      Vector whereConditions,selectTables;
      Object whereObj;
      boolean leftConstant,rightConstant;
      String objectType,rightColumn,leftColumn,rightString,leftString;
      String[] whereCondition;
      selectTables = (Vector)tables.get("TABLE_SELECT_ORDER");
      for ( i = 0 ; i < whereClauseList.size(); i++ )
      {
         whereConditions = (Vector)whereClauseList.elementAt(i);
         for ( j = 0; j < whereConditions.size(); j++ )
         {
/*
 *          Where conditions can be tinySQLWhere objects or String arrays.
 */
            whereObj = whereConditions.elementAt(j);
            objectType = whereObj.getClass().getName();
            if ( objectType.endsWith("tinySQLWhere") )
            {
               ((tinySQLWhere)whereObj).setColumnTypes(tables);
            } else if ( objectType.endsWith("java.lang.String;") ) {
               whereCondition = (String[])whereObj;
               leftColumn = whereCondition[1];
               leftString = whereCondition[5];
/*
 *             Try and find a table for this column.  If one is found, 
 *             change the column to include the full table name and alias.
 */
               leftTable = getTableForColumn(tables,leftColumn);
               leftConstant = false;
               if ( leftTable != (tinySQLTable)null )
               {
                  leftColumn = leftColumn.toUpperCase();
                  leftType = leftTable.ColType(leftColumn);
                  dotAt = leftColumn.indexOf(".");
                  if ( dotAt > -1 ) leftColumn = leftColumn.substring(dotAt+1);
                  leftColumn = leftTable.table + "->" + leftTable.tableAlias
                     + "." + leftColumn;
                  whereCondition[1] = leftColumn; 
               } else {
/*
 *                No table could be found for this column.  Treat it as a 
 *                constant.
 */
                  if ( debug )
                     System.out.println("No table found for " + leftColumn
                     + " - treat as constant");
                  leftType = UtilString.getValueType(leftColumn);
                  leftString = UtilString.removeQuotes(leftColumn);
                  leftConstant = true;
               }
               rightColumn = whereCondition[3];
               rightString = whereCondition[7];
               rightTable = getTableForColumn(tables,rightColumn);
               rightConstant = false;
               if ( rightTable != (tinySQLTable)null )
               {
                  rightColumn = rightColumn.toUpperCase();
                  rightType = rightTable.ColType(rightColumn);
                  dotAt = rightColumn.indexOf(".");
                  if ( dotAt > -1 )
                     rightColumn = rightColumn.substring(dotAt+1);
                  rightColumn = rightTable.table + "->" 
                     + rightTable.tableAlias + "." + rightColumn;
                  whereCondition[3] = rightColumn; 
               } else {
/*
 *                No table could be found for this column.  Treat it as a 
 *                constant.
 */
                  if ( debug )
                     System.out.println("No table found for " 
                     + rightColumn + " - treat as constant");
                  rightType = UtilString.getValueType(rightColumn);
                  rightString = UtilString.removeQuotes(rightColumn);
                  rightConstant = true;
               }
/*
 *             Check to make sure that the left and right parts of the
 *             where condition are compatible.
 */
               if ( Utils.isCharColumn(leftType) )
               {
                  if ( !Utils.isCharColumn(rightType) )
                     throw new tinySQLException("Incompatible types: left is"
                     + " character but right is not.");
/*
 *                For constant values, set the type and make the value equal
 *                to the parameter name.  Any tables involved with where
 *                conditions which contain a constant are bumped to the
 *                top of the selection stack to improve efficiency.
 */
                  whereCondition[4] = "CHAR";
                  if ( leftConstant ) 
                  {
                     whereCondition[4] = "CHAR_CONSTANT";
                     whereCondition[5] = leftString;
                     whereCondition[8] = "LEFT";
                     if ( rightTable != (tinySQLTable)null )
                        Utils.setPriority(selectTables,rightTable.table);
                  }
                  whereCondition[6] = "CHAR";
                  if ( rightConstant ) 
                  {
                     whereCondition[6] = "CHAR_CONSTANT";
                     whereCondition[7] = rightString;
                     if ( whereCondition[8].equals("LEFT") )
                        whereCondition[8] = "BOTH";
                     else
                        whereCondition[8] = "RIGHT";
                     if ( leftTable != (tinySQLTable)null )
                        Utils.setPriority(selectTables,leftTable.table);
                  }
               } else if ( Utils.isNumberColumn(leftType) ) {
                  if ( !Utils.isNumberColumn(rightType) )
                     throw new tinySQLException("Incompatible types: left is "
                     + " numeric but right is not.");
                  whereCondition[4] = "NUMBER";
                  if ( leftConstant )
                  {
                     whereCondition[4] = "NUMBER_CONSTANT";
                     whereCondition[5] = whereCondition[1];
                     whereCondition[8] = "LEFT";
                  }
                  whereCondition[6] = "NUMBER";
                  if ( rightConstant ) 
                  {
                     whereCondition[6] = "NUMBER_CONSTANT";
                     whereCondition[7] = whereCondition[3];
                     if ( whereCondition[8].equals("LEFT") )
                        whereCondition[8] = "BOTH";
                     else
                        whereCondition[8] = "RIGHT";
                  }
               }
            }
         }
      }
   }
/*
 * Clear all the non-constant values in all where conditions
 */
   public void clearValues(String inputTableName)
   {
      int i,j,dotAt;
      Vector whereConditions;
      Object whereObj;
      String objectType,columnName,tableName;
      String[] whereCondition;
      StringBuffer outputBuffer = new StringBuffer();
      boolean conditionCleared;
      for ( i = 0 ; i < whereClauseList.size(); i++ )
      {
         whereConditions = (Vector)whereClauseList.elementAt(i);
         for ( j = 0; j < whereConditions.size(); j++ )
         {
/*
 *          Where conditions can be tinySQLWhere objects or String arrays.
 */
            whereObj = whereConditions.elementAt(j);
            objectType = whereObj.getClass().getName();
            conditionCleared = false;
            if ( objectType.endsWith("tinySQLWhere") )
            {
               ((tinySQLWhere)whereObj).clearValues(inputTableName);
            } else if ( objectType.endsWith("java.lang.String;") ) {
               whereCondition = (String[])whereObj;
               if ( whereCondition[8].equals("UNKNOWN") ) continue;
/*
 *             Check left side of condition
 */
               dotAt = whereCondition[1].indexOf(".");
               if ( !whereCondition[4].endsWith("CONSTANT") & dotAt > -1 )
               {
                  tableName = whereCondition[1].substring(0,dotAt);
                  if ( tableName.equals(inputTableName) )
                  {
                     whereCondition[5] = (String)null;
                     if ( whereCondition[8].equals("LEFT") )
                        whereCondition[8] = "UNKNOWN";
                     else
                        whereCondition[8] = "RIGHT";
                     conditionCleared = true;
                  }
               } 
/*
 *             Check right side of condition
 */
               dotAt = whereCondition[3].indexOf(".");
               if ( !whereCondition[6].endsWith("CONSTANT") & dotAt > -1 )
               {
                  tableName = whereCondition[3].substring(0,dotAt);
                  if ( tableName.equals(inputTableName) )
                  {
                     whereCondition[7] = (String)null;
                     if ( whereCondition[8].equals("RIGHT") )
                        whereCondition[8] = "UNKNOWN";
                     else
                        whereCondition[8] = "LEFT";
                     conditionCleared = true;
                  }
               }
               if ( debug & conditionCleared )
                 System.out.println("Where condition after clearing " 
                 + inputTableName + " is\n" + conditionToString(whereCondition));
            }
         }
      }
   }
   public String toString()
   {
      int i,j;
      Vector whereConditions;
      Object whereObj;
      String objectType;
      String[] whereCondition;
      StringBuffer outputBuffer = new StringBuffer();
      for ( i = 0 ; i < whereClauseList.size(); i++ )
      {
         if ( i > 0 ) outputBuffer.append("OR\n");
         whereConditions = (Vector)whereClauseList.elementAt(i);
         for ( j = 0; j < whereConditions.size(); j++ )
         {
            if ( j > 0 ) outputBuffer.append("AND\n");
/*
 *          Where conditions can be tinySQLWhere objects or String arrays.
 */
            whereObj = whereConditions.elementAt(j);
            objectType = whereObj.getClass().getName();
            if ( objectType.endsWith("tinySQLWhere") )
            {
               outputBuffer.append(((tinySQLWhere)whereObj).toString());
            } if ( objectType.endsWith("java.lang.String;") ) {
               whereCondition = (String[])whereObj;
               outputBuffer.append(conditionToString(whereCondition) + "\n");
            }
         }
      }
      return outputBuffer.toString();
   }
/*
 * Format a where condition for display.
 */
   private String conditionToString(String[] inputWhereCondition)
   {
      int i;
      StringBuffer outputBuffer = new StringBuffer();;
      for ( i = 0; i < 9; i++ )
      {
          if ( inputWhereCondition[i] == (String)null )
             outputBuffer.append(" NULL ");
          else 
             outputBuffer.append(" " + inputWhereCondition[i] + " ");
      }
      return outputBuffer.toString();
   }
      
/*
 * Given a column name, and a Hashtable containing tables, determine
 * which table "owns" a given column. 
 */
   private tinySQLTable getTableForColumn(Hashtable tables, String inputColumn)
   {
      tinySQLTable tbl;
      Vector tableNames;
      Hashtable columnInfo;
      String findColumn,tableAndAlias=(String)null,tableAlias;
      int i,dotAt;
      findColumn = inputColumn.toUpperCase();
      dotAt = findColumn.indexOf(".");
      tableNames = (Vector)tables.get("TABLE_SELECT_ORDER");
      if ( dotAt > -1 ) 
      {
         tableAlias = findColumn.substring(0,dotAt);
         try
         {
            tableAndAlias = UtilString.findTableAlias(tableAlias,tableNames);
         } catch (Exception ex ) {
         }
         if ( tableAndAlias != (String)null )
         {
            tbl = (tinySQLTable)tables.get(tableAndAlias);
            if ( tbl != (tinySQLTable)null ) return tbl;
         }
      } else {
         for ( i = 0; i < tableNames.size(); i++ )
         {
            tbl = (tinySQLTable)tables.get((String)tableNames.elementAt(i));
/*
 *          Get the Hashtable containing column information, and see if it
 *          contains the column we're looking for.
 */
            columnInfo = tbl.column_info;
            if ( columnInfo != (Hashtable)null )
               if (columnInfo.containsKey(findColumn)) return tbl;
         }
      }
      return (tinySQLTable)null;
   }
/*
 * This method updates the where conditions that contain the input column and
 * returns the status of the entire where clause.
 */  
   public String evaluate(String inputColumnName,String inputColumnValue)
      throws tinySQLException
   {
      int i,j,k;
      FieldTokenizer ft;
      Vector whereConditions;
      Object whereObj;
      String objectType,leftString,rightString,comparison,conditionStatus,
      nextField;
      String[] whereCondition;
      boolean like,whereUpdated;
      double leftValue, rightValue;
      for ( i = 0 ; i < whereClauseList.size(); i++ )
      {
         whereConditions = (Vector)whereClauseList.elementAt(i);
         for ( j = 0; j < whereConditions.size(); j++ )
         {
/*
 *          Where conditions can be tinySQLWhere objects or String arrays.
 */
            conditionStatus = "TRUE";
            whereObj = whereConditions.elementAt(j);
            objectType = whereObj.getClass().getName();
            whereUpdated = false;
            if ( objectType.endsWith("tinySQLWhere") )
            {
               conditionStatus =((tinySQLWhere)whereObj).evaluate(inputColumnName,inputColumnValue);
            } else if ( objectType.endsWith("java.lang.String;") ) {
               whereCondition = (String[])whereObj;
/*
 *             Check for updates on this column.  Update the status to 
 *             reflect which parts of the where condition have been set.
 */
               if ( inputColumnName.equals(whereCondition[1]) )
               {
                  whereUpdated = true;
                  whereCondition[5] = inputColumnValue.trim();
                  if ( whereCondition[8].equals("UNKNOWN") )
                     whereCondition[8] = "LEFT";
                  else if ( whereCondition[8].equals("RIGHT") )
                     whereCondition[8] = "BOTH";
               } else if ( inputColumnName.equals(whereCondition[3]) ) {
                  whereUpdated = true;
                  whereCondition[7] = inputColumnValue.trim();
                  if ( whereCondition[8].equals("UNKNOWN") )
                     whereCondition[8] = "RIGHT";
                  else if ( whereCondition[8].equals("LEFT") )
                     whereCondition[8] = "BOTH";
               }
               if ( !whereUpdated ) continue;
               if ( debug ) System.out.println("Where condition update to " 
               + inputColumnName + "\n" + conditionToString(whereCondition));
/*
 *             A where condition cannot be evaluated until both left and 
 *             right values have been assigned.
 */
               if ( whereCondition[8].equals("UNKNOWN") |
                    whereCondition[8].equals("LEFT") |    
                    whereCondition[8].equals("RIGHT") ) continue;
/*
 *             Evaluate this where condition.
 */
               leftString = whereCondition[5];
               rightString = whereCondition[7];
               comparison = whereCondition[2];
               if ( whereCondition[4].startsWith("CHAR") )
               {
                  if ( comparison.equals("=") &
                              !leftString.equals(rightString) ) {
                     conditionStatus = "FALSE";
                  } else if ( comparison.equalsIgnoreCase("LIKE") ) {
                     ft = new FieldTokenizer(rightString,'%',false);
                     like = false;
                     while ( ft.hasMoreFields() )
                     {
                        nextField = ft.nextField();
                        if ( leftString.indexOf(nextField) > -1 )
                           like = true;
                     }
                     if ( !like ) conditionStatus = "FALSE";
                  } else if ( comparison.equals("<>") &
                              leftString.equals(rightString) ) {
                     conditionStatus = "FALSE";
                  } else if ( comparison.equals("!=") &
                              leftString.equals(rightString) ) {
                     conditionStatus = "FALSE";
                  } else if ( comparison.equals(">") &
                              leftString.compareTo(rightString) <= 0 ) {
                     conditionStatus = "FALSE";
                  } else if ( comparison.equals("<") &
                              leftString.compareTo(rightString) >= 0 ) {
                     conditionStatus = "FALSE";
                  }
               } else if ( whereCondition[4].startsWith("NUMBER") ) {
/*
 *                Try to convert the strings to numeric values
 */         
                  try 
                  {
                     leftValue = Double.parseDouble(leftString);
                  } catch (Exception e) {
                     throw new tinySQLException( e.getMessage() + 
                     ": Could not convert [" + leftString + "] to numeric.");
                  }
                  try
                  {
                     rightValue = Double.parseDouble(rightString);
                  } catch (Exception e) {
                     throw new tinySQLException( e.getMessage() + 
                     ": Could not convert [" + rightString + "] to numeric.");
                  }
/* 
 *                Check the comparison.
 */
                  if (comparison.equals("=") & leftValue != rightValue)
                     conditionStatus = "FALSE";
                  else if (comparison.equals("<>") & leftValue == rightValue)
                     conditionStatus = "FALSE";
                  else if (comparison.equals(">") & leftValue <= rightValue)
                     conditionStatus = "FALSE";
                  else if (comparison.equals("<") & leftValue >= rightValue)
                     conditionStatus = "FALSE";
                  else if (comparison.equals("<=") & leftValue > rightValue)
                     conditionStatus = "FALSE";
                  else if (comparison.equals("=<") & leftValue > rightValue)
                     conditionStatus = "FALSE";
                  else if (comparison.equals(">=") & leftValue < rightValue)
                     conditionStatus = "FALSE";
                  else if (comparison.equals("=>") & leftValue < rightValue)
                     conditionStatus = "FALSE";
               }
               whereCondition[8] = conditionStatus;
               if ( debug ) System.out.println("Where condition evaluation:\n" 
               + conditionToString(whereCondition)); 
            }
         }
      }
      return getStatus();
   }
/*
 * This method evaluates the status of the entire where clause.
 */
   public String getStatus()
   {
      int i,j;
      Vector whereConditions;
      Object whereObj;
      String objectType,andStatus,orStatus;
      String[] whereCondition;
      orStatus = "FALSE";
      for ( i = 0 ; i < whereClauseList.size(); i++ )
      {
/*
 *       The AND operator is applied to the whereConditions
 */
         whereConditions = (Vector)whereClauseList.elementAt(i);
         andStatus = "TRUE";
         for ( j = 0; j < whereConditions.size(); j++ )
         {
/*
 *          Where conditions can be tinySQLWhere objects or String arrays.
 */
            whereObj = whereConditions.elementAt(j);
            objectType = whereObj.getClass().getName();
            if ( objectType.endsWith("tinySQLWhere") )
            {
               andStatus = ((tinySQLWhere)whereObj).getStatus();
               if ( andStatus.equals("FALSE")) break;
            } else if ( objectType.endsWith("java.lang.String;") ) {
               whereCondition = (String[])whereObj;
/*
 *             If any AND condition is FALSE, the entire where condition 
 *             is FALSE.
 */
               if ( whereCondition[8].equals("FALSE") )
               {
                  andStatus = "FALSE";
                  break;
/*
 *             If any AND condition is UNKNOWN, LEFT, or RIGHT, the entire
 *             where condition is UNKNOWN.
 */
               } else if ( whereCondition[8].equals("UNKNOWN") |
                           whereCondition[8].equals("LEFT") |
                           whereCondition[8].equals("RIGHT") ) {
                  andStatus = "UNKNOWN";
               }
            }
         }
/*
 *       If any OR condition is true, the entire where condition
 *       is true
 */
         if ( andStatus.equals("TRUE") ) 
         {
            orStatus = "TRUE";
            break;
         } else if ( andStatus.equals("UNKNOWN") ) {
            orStatus = "UNKNOWN";
         }
      }
      if ( debug ) System.out.println("Return status " + orStatus);
      return orStatus;
   }
}                   
