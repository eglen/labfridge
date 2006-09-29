/*
 * tinySQLParser
 * 
 * $Author: davis $
 * $Date: 2004/12/18 21:28:17 $
 * $Revision: 1.1 $
 *
 * This simple token based parser replaces the CUP generated parser
 * simplifying extensions and reducing the total amount of code in 
 * tinySQL considerably.
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
 * Revision History;
 *
 * Written by Davis Swan in April, 2004.
 */

package com.sqlmagic.tinysql;

import java.io.*;
import java.util.*;
import java.text.*;
import java.sql.Types;

public class tinySQLParser
{
   Vector columnList,tableList,actionList,valueList,contextList,
   columnAliasList;
   tinySQLWhere whereClause;
   String statementType=(String)null,tableName=(String)null;
   String lastKeyWord=(String)null,orderType=(String)null;
   String oldColumnName=(String)null,newColumnName=(String)null;
   String[] colTypeNames = {"INT","FLOAT","CHAR","DATE"};
   int[] colTypes = {Types.INTEGER,Types.FLOAT,Types.CHAR,Types.DATE};
   int starAt = Integer.MIN_VALUE;
   boolean debug=false;
   public tinySQLParser(InputStream sqlInput) throws tinySQLException
   {
      StreamTokenizer st;
      FieldTokenizer ft;
      Reader r;
      String nextToken,upperField,nextField,keyWord=(String)null;
      StringBuffer cmdBuffer,inputSQLBuffer;
      int lastIndex,keyIndex;
      r = new BufferedReader(new InputStreamReader(sqlInput));
      actionList = new Vector();
      columnList = new Vector();
      columnAliasList = new Vector();
      contextList = new Vector();
      tableList = new Vector();
      valueList = new Vector();
      tableName = (String)null;
      whereClause = (tinySQLWhere)null;
      try
      {
         st = new StreamTokenizer(r);
         st.eolIsSignificant(false);
         st.wordChars('\'','}');
         st.wordChars('?','?');
         st.wordChars('"','.');
         st.ordinaryChars('0','9');
         st.wordChars('0','9');
         cmdBuffer = new StringBuffer();
         inputSQLBuffer = new StringBuffer();
         while ( st.nextToken() != StreamTokenizer.TT_EOF)
         {
            if ( st.ttype == StreamTokenizer.TT_WORD )
               nextToken = st.sval.trim();
            else 
               continue;
            if ( inputSQLBuffer.length() > 0 ) inputSQLBuffer.append(" ");
            inputSQLBuffer.append(nextToken);
         }
         ft = new FieldTokenizer(inputSQLBuffer.toString(),' ',false);
         while ( ft.hasMoreFields() )
         {
            nextField = ft.nextField();
            upperField = nextField.toUpperCase();
            if ( statementType == (String)null ) 
            {
               statementType = upperField;
               lastIndex = getKeywordIndex(statementType,statementType);
               if ( lastIndex != 0 ) throwException(9);
               keyWord = statementType;
            } else {
               keyIndex = getKeywordIndex(statementType,upperField);
               if ( keyIndex < 0 )
               {
                  if ( cmdBuffer.length() > 0 ) cmdBuffer.append(" ");
                  cmdBuffer.append(nextField);
               } else {
                  setPhrase(keyWord,cmdBuffer.toString());
                  cmdBuffer = new StringBuffer();
                  keyWord = upperField;
                  if ( debug ) System.out.println("Found keyword " + keyWord);
               }
            }  
         }
         if ( keyWord != (String)null ) setPhrase(keyWord,cmdBuffer.toString());
         addAction();
         if ( debug ) System.out.println("SQL:"+inputSQLBuffer.toString());
      } catch ( Exception ex ) {
         throw new tinySQLException(ex.getMessage());
      }
   }
/*
 * This method sets up particular phrase elements for the SQL command.
 * Examples would be a list of selected columns and tables for a SELECT
 * statement, or a list of column definitions for a CREATE TABLE
 * statement.  These phrase elements will be added to the action list
 * once the entire statement has been parsed.
 */
   public void setPhrase(String inputKeyWord,String inputString)
      throws tinySQLException
   {
      String nextField,upperField,tableAlias,colTypeStr,colTypeSpec,
      fieldString,syntaxErr,tempString,columnName,columnAlias;
      StringBuffer colTypeBuffer,concatBuffer;
      FieldTokenizer ft1,ft2,ft3;
      tsColumn createColumn;
      int i,j,k,lenc,colType,countFields;
/*
 *    Handle compound keywords.
 */
      if ( inputString == (String)null ) 
      {
         lastKeyWord = inputKeyWord;
         return;
           
      } else if ( inputString.trim().length() == 0 ) {
         lastKeyWord = inputKeyWord;
         return;
      }
      ft1 = new FieldTokenizer(inputString,',',false);
      while ( ft1.hasMoreFields() )
      {
         nextField = ft1.nextField().trim();
         if ( debug ) System.out.println("Next field is " + nextField);
         upperField = nextField.toUpperCase();
         if ( inputKeyWord.equals("SELECT") )
         {
/*
 *          Check for and set column alias.
 */
            ft2 = new FieldTokenizer(nextField,' ',false);
            columnName = ft2.getField(0);
            if ( columnName.equals("*") ) starAt = columnList.size();
            columnAlias = (String)null;
/*
 *          A column alias can be preceded by the keyword AS which will
 *          be ignored by tinySQL.
 */
            if ( ft2.countFields() == 2 ) columnAlias = ft2.getField(1);
            else if ( ft2.countFields() == 3 ) columnAlias = ft2.getField(2);
/*
 *          Check for column concatenation using the | symbol
 */
            ft2 = new FieldTokenizer(columnName,'|',false);
            if ( ft2.countFields() > 1 ) 
            {
               concatBuffer = new StringBuffer("CONCAT(");
               while ( ft2.hasMoreFields() )
               {
                  if ( concatBuffer.length() > 7 ) 
                     concatBuffer.append(",");
                  concatBuffer.append(ft2.nextField());
               }
               columnName = concatBuffer.toString() + ")";
            }
            columnList.addElement(columnName);
            columnAliasList.addElement(columnAlias);
            contextList.addElement(inputKeyWord);
         } else if ( inputKeyWord.equals("TABLE") ) {
/*
 *          If the input keyword is TABLE, update the statement type to be a 
 *          compound type such as CREATE_TABLE, DROP_TABLE, or ALTER_TABLE.
 */
            if ( !statementType.equals("INSERT") )
               statementType = statementType + "_TABLE";
            if ( statementType.equals("CREATE_TABLE") ) 
            {
/*
 *             Parse out the column definition.
 */
               ft2 = new FieldTokenizer(nextField,'(',false);
               if ( ft2.countFields() != 2 ) throwException(1);
               tableName = ft2.getField(0);
               fieldString = ft2.getField(1);
               ft2 = new FieldTokenizer(fieldString,',',false);
               while ( ft2.hasMoreFields() )
               {
                  tempString = ft2.nextField();
                  createColumn = parseColumnDefn(tempString);
                  if ( createColumn != (tsColumn)null )
                     columnList.addElement(createColumn);
               }
            } else {
               tableName = upperField;
            } 
         } else if ( inputKeyWord.equals("BY") ) {
/*
 *          Set up Group by and Order by columns.
 */
            if ( lastKeyWord == (String)null ) 
            {
               throwException(6);
            } else {
               ft3 = new FieldTokenizer(upperField,' ',false);
               columnList.addElement(ft3.getField(0));
               if ( ft3.countFields() == 2 )
               {
/*
 *                ASC or DESC are the only allowable directives after GROUP BY
 */
                  if ( ft3.getField(1).startsWith("ASC") |
                       ft3.getField(1).startsWith("DESC") )
                     orderType = ft3.getField(1);
                  else
                     throwException(7);
               }
               contextList.addElement(lastKeyWord);
            }
         } else if ( inputKeyWord.equals("DROP") ) {
/*
 *          Parse list of columns to be dropped.
 */
            statementType = "ALTER_DROP";
            ft2 = new FieldTokenizer(upperField,' ',false);
            while ( ft2.hasMoreFields() )
            {
               columnList.addElement(UtilString.removeQuotes(ft2.nextField()));
            }
         } else if ( inputKeyWord.equals("RENAME") ) {
/*
 *          Parse old and new column name.
 */
            statementType = "ALTER_RENAME";
            ft2 = new FieldTokenizer(upperField,' ',false);
            oldColumnName = ft2.getField(0);
            newColumnName = ft2.getField(1);
         } else if ( inputKeyWord.equals("ADD") ) {
/*
 *          Parse definition of columns to be added.
 */
            statementType = "ALTER_ADD";
            createColumn = parseColumnDefn(nextField);
            if ( createColumn != (tsColumn)null )
               columnList.addElement(createColumn);
         } else if ( inputKeyWord.equals("FROM") ) {
/*
 *          Check for and set table alias.
 */
            ft2 = new FieldTokenizer(upperField,' ',false);
            tableName = ft2.getField(0);
            tableAlias = (ft2.getField(1,tableName)).toUpperCase();
            tableList.addElement(tableName + "->" + tableAlias);
         } else if ( inputKeyWord.equals("INTO") ) {
            ft2 = new FieldTokenizer(nextField,'(',false);
            if ( ft2.countFields() != 2 ) throwException(3);
            tableName = ft2.getField(0);
            fieldString = ft2.getField(1).toUpperCase();
            ft2 = new FieldTokenizer(fieldString,',',false);
            while ( ft2.hasMoreFields() )
            {
               tempString = UtilString.removeQuotes(ft2.nextField());
               columnList.addElement(tempString);
               contextList.addElement(inputKeyWord);
            }
         } else if ( inputKeyWord.equals("VALUES") ) {
            ft2 = new FieldTokenizer(nextField,'(',false);
            fieldString = ft2.getField(0);
            ft2 = new FieldTokenizer(fieldString,',',false);
            while ( ft2.hasMoreFields() )
            {
               tempString = UtilString.removeQuotes(ft2.nextField());
               tempString = UtilString.replaceAll(tempString,"''","'");
               valueList.addElement(tempString);
            }
         } else if ( inputKeyWord.equals("UPDATE") ) {
            tableName = nextField.toUpperCase();
         } else if ( inputKeyWord.equals("SET") ) {
/*
 *          Parse the update column name/value pairs
 */
            ft2 = new FieldTokenizer(nextField,'=',false);
            if ( ft2.countFields() != 2 ) throwException(4);
            columnList.addElement(ft2.getField(0));
            contextList.addElement(inputKeyWord);
            valueList.addElement(UtilString.removeQuotes(ft2.getField(1)));
         } else if ( inputKeyWord.equals("WHERE") ) {
            whereClause = new tinySQLWhere(nextField);
         } else if ( !inputKeyWord.equals("TABLE") ) {
            throwException(10);
         }
      }
      lastKeyWord = inputKeyWord;
   }
   public tsColumn parseColumnDefn(String columnDefn) throws tinySQLException
   {
/*
 *    Parse out the column definition.
 */
      tsColumn createColumn;
      int i;
      FieldTokenizer ft;
      String fieldString,tempString,colTypeStr,colTypeSpec;
      ft = new FieldTokenizer(columnDefn.toUpperCase(),' ',false);
/*
 *    A column definition must consist of a column name followed by a 
 *    column specification.
 */
      if ( ft.countFields() < 2 ) throwException(2);
      createColumn = new tsColumn(ft.getField(0));
      colTypeStr = "";
      for ( i = 1; i < ft.countFields(); i++ )
         colTypeStr += ft.getField(1);
      ft = new FieldTokenizer(colTypeStr,'(',false);
      colTypeStr = ft.getField(0);
      createColumn.size = 10;
      createColumn.decimalPlaces = 0;
      if ( colTypeStr.equals("FLOAT") ) 
      {
         createColumn.size = 12;
         createColumn.decimalPlaces = 2;
      }
      colTypeSpec = ft.getField(1);
      if ( !colTypeSpec.equals("NULL") )
      {
/*
 *       Parse out the scale and precision is supplied.
 */
         ft = new FieldTokenizer(colTypeSpec,',',false);
         createColumn.size = ft.getInt(0,8);
         createColumn.decimalPlaces = ft.getInt(1,0);
      }
      createColumn.type = Integer.MIN_VALUE;
      for ( i = 0; i < colTypeNames.length; i++ )
         if ( colTypeStr.equals(colTypeNames[i]) )
             createColumn.type = colTypes[i]; 
      if ( createColumn.type == Integer.MIN_VALUE ) throwException(8);
      if ( debug ) System.out.println("Column " + createColumn.name 
      + ", type is " + createColumn.type + ",size is " + createColumn.size 
      + ",precision is " + createColumn.decimalPlaces);
      return createColumn;
   }
/*
 * This method is used to idenify SQL key words, and the order in which they
 * should appear in the SQL statement.
 */
   public int getKeywordIndex(String inputContext,String inputWord)
   {
      String[][] sqlSyntax = {{"SELECT","FROM","WHERE","GROUP","ORDER","BY"},
        {"INSERT","INTO","VALUES"},
        {"DROP","TABLE"},
        {"DELETE","FROM","WHERE"},
        {"CREATE","TABLE"},
        {"UPDATE","SET","WHERE"},
        {"ALTER","TABLE","DROP","MODIFY","ADD","RENAME"}};
      int i,j;
      for ( i = 0; i < sqlSyntax.length; i++ )
      {
         for ( j = 0; j < sqlSyntax[i].length; j++ )
         {
            if ( sqlSyntax[i][0].equals(inputContext) &
                 sqlSyntax[i][j].equals(inputWord) )
               return j;
         }
      }
      return Integer.MIN_VALUE;
   }
/*
 * Add an action Hashtable to the list of actions
 */
   public void addAction () throws tinySQLException
   {
      int i,j,foundDot;
      String columnName;
      Hashtable newAction =  new Hashtable();
      newAction.put("TYPE", statementType);
      if ( statementType.equals("SELECT") )
      {
         newAction.put("TABLES",tableList);
         if ( whereClause != (tinySQLWhere)null )
            newAction.put("WHERE",whereClause);
/*
 *       If this is a SELECT *, move the * to the bottom of the columnList
 *       so that the column expansion will not impact GROUP BY or
 *       ORDER BY columns.
 */
         if ( starAt != Integer.MIN_VALUE ) 
         {
            columnList.addElement(columnList.elementAt(starAt));
            columnAliasList.addElement(columnList.elementAt(starAt));
            contextList.addElement(contextList.elementAt(starAt));
            columnList.removeElementAt(starAt);
            columnAliasList.removeElementAt(starAt);
            contextList.removeElementAt(starAt);
         }
         newAction.put("COLUMNS",columnList);
         newAction.put("COLUMN_ALIASES",columnAliasList);
         newAction.put("CONTEXT",contextList);
         if ( orderType != (String)null ) newAction.put("ORDER_TYPE",orderType);
      } else if ( statementType.equals("DROP_TABLE") ) {
         newAction.put("TABLE",tableName);
      } else if ( statementType.equals("CREATE_TABLE") ) {
         newAction.put("TABLE",tableName);
         newAction.put("COLUMN_DEF",columnList);
      } else if ( statementType.equals("ALTER_RENAME") ) {
         newAction.put("OLD_COLUMN",oldColumnName);
         newAction.put("NEW_COLUMN",newColumnName);
      } else if ( statementType.equals("ALTER_ADD") ) {
         newAction.put("TABLE",tableName);
         newAction.put("COLUMN_DEF",columnList);
      } else if ( statementType.equals("ALTER_DROP") ) {
         newAction.put("TABLE",tableName);
         newAction.put("COLUMNS",columnList);
      } else if ( statementType.equals("DELETE") ) {
         newAction.put("TABLE",tableName);
         if ( whereClause != (tinySQLWhere)null )
            newAction.put("WHERE",whereClause);
      } else if ( statementType.equals("INSERT") |
                  statementType.equals("UPDATE") ) {
         newAction.put("TABLE",tableName);
         if ( columnList.size() != valueList.size() ) throwException(5);
         newAction.put("COLUMNS",columnList);
         newAction.put("VALUES",valueList);
         if ( whereClause != (tinySQLWhere)null )
            newAction.put("WHERE",whereClause);
      }
      actionList.addElement(newAction);
   }
   public void throwException(int exceptionNumber) throws tinySQLException
   {
      String exMsg = (String)null;
      if ( exceptionNumber == 1 )
         exMsg = "CREATE TABLE must be followed by a table name and a list"
         + " of column specifications enclosed in brackets.";
      else if ( exceptionNumber == 2 )
         exMsg = "A column specification must consist of a column name"
         + " followed by a column type specification.";
      else if ( exceptionNumber == 3 )
         exMsg = "INTO should be followed by a table name and "
         + "a list of columns enclosed in backets.";
      else if ( exceptionNumber == 4 )
         exMsg = "SET must be followed by assignments in the form"
         + " <columnName>=<value>.";
      else if ( exceptionNumber == 5 )
         exMsg = "INSERT statement number of columns and values provided"
         + " do not match.";
      else if ( exceptionNumber == 6 )
         exMsg = "BY cannot be the first keyword.";
      else if ( exceptionNumber == 7 )
         exMsg = "ORDER BY can only be followed by the ASC or DESC directives";
      else if ( exceptionNumber == 8 )
         exMsg = "Supported column types are INT,CHAR,FLOAT,DATE";
      else if ( exceptionNumber == 9 )
         exMsg = "Expecting SELECT, INSERT, ALTER, etc. in " + statementType;
      else if ( exceptionNumber == 10 )
         exMsg = "Unrecognized keyword ";
      throw new tinySQLException(exMsg);
   }
   public Vector getActions()
   {
      return actionList;
   }
}
