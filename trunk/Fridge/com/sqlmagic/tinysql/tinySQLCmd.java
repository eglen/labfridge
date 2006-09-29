/*
 * Java program to execute SQL commands using the tinySQL JDBC driver.
 *
 * $Author: davis $
 * $Date: 2004/12/18 21:26:02 $
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
 * Written by Davis Swan in November, 2003.
 */

package com.sqlmagic.tinysql;

import java.io.*;
import java.sql.*;
import java.net.*;
import java.util.*;
public class tinySQLCmd
{
   static Vector tableList;
   static String dbVersion;
   public static void main(String[] args) throws IOException,SQLException
   {
      DatabaseMetaData dbMeta;
      ResultSetMetaData meta;
      ResultSet display_rs,typesRS;
      BufferedReader stdin,loadFileReader;
      BufferedReader startReader=(BufferedReader)null;
      Connection con;
      Statement stmt;
      FieldTokenizer ft;
      PreparedStatement pstmt=(PreparedStatement)null;
      int i,rsColCount,endAt,colWidth,colScale,colPrecision,typeCount,
      colType,parameterIndex,b1,b2,parameterInt,startAt,columnIndex,valueIndex;
      String fName,tableName=null,inputString,cmdString,colTypeName,dbType,
      parameterString,loadString,fieldString,readString;
      StringBuffer lineOut,prepareBuffer,valuesBuffer,inputBuffer;
      boolean debug=false;
      stdin = new BufferedReader(new InputStreamReader(System.in));
      try 
      {
/*
 *       Register the JDBC driver for dBase
 */
         Class.forName("com.sqlmagic.tinysql.dbfFileDriver");
      } catch (ClassNotFoundException e) {
         System.err.println(
              "JDBC Driver could not be registered!!\n");
         if ( debug ) e.printStackTrace();
      }
      fName = ".";
      if ( args.length > 0 ) fName = args[0];
/* 
 *    Establish a connection to dBase
 */
      con = dbConnect(fName);
      if ( con == (Connection)null )
      {
         fName = ".";
         con = dbConnect(fName);
      }
      dbMeta = con.getMetaData();
      dbType = dbMeta.getDatabaseProductName();		
      dbVersion = dbMeta.getDatabaseProductVersion();		
      System.out.println("===========================================");
      System.out.println(dbType + " Command line interface version " 
      + dbVersion);
      System.out.println("Type HELP to get information on available commands.");
      cmdString = "NULL";
      stmt = con.createStatement();
      inputString = (String)null;
      if ( args.length > 1 ) inputString = args[1].trim();
      while ( !cmdString.toUpperCase().equals("EXIT") )
      {
         try
         {
            if ( startReader != (BufferedReader)null )
            {
/*
 *             Command START files can contain comments and can have
 *             commands broken over several lines.  However, they 
 *             cannot have partial commands on a line.
 */
               inputBuffer = new StringBuffer();
               inputString = (String)null;
               while ( ( readString = startReader.readLine() ) != null )
               {
                  if ( readString.startsWith("--") |
                       readString.startsWith("#") ) continue;
                  inputBuffer.append(readString + " ");
/*
 *                A field tokenizer must be used to avoid problems with
 *                semi-colons inside quoted strings.
 */
                  ft = new FieldTokenizer(inputBuffer.toString(),';',true);
                  if ( ft.countFields() > 1 )
                  {
                     inputString = inputBuffer.toString();
                     break;
                  }
               }
               if ( inputString == (String)null ) 
               {
                  startReader = (BufferedReader)null;
                  continue;
               }
            } else if ( args.length == 0 ) {
               System.out.print("tinySQL>");
               inputString = stdin.readLine().trim();
            }
            if ( inputString == (String)null ) break;
            if (inputString.toUpperCase().startsWith("EXIT") |
                inputString.toUpperCase().startsWith("QUIT") ) break;
            startAt = 0;
            while ( startAt < inputString.length() - 1 )
            {
               endAt = inputString.indexOf(";",startAt);
               if ( endAt == -1 )
                  endAt = inputString.length();
               cmdString = inputString.substring(startAt,endAt);
               startAt = endAt + 1;
               if ( cmdString.toUpperCase().startsWith("SELECT") ) 
               {
                  display_rs = stmt.executeQuery(cmdString);
                  if ( display_rs == (ResultSet)null )
                  {
                     System.out.println("Null ResultSet returned from query");
                     continue;
                  }
                  meta = display_rs.getMetaData();
/*
 *                The actual number of columns retrieved has to be checked
 */
                  rsColCount = meta.getColumnCount();
                  lineOut = new StringBuffer(100);
                  int[] columnWidths = new int[rsColCount];
                  int[] columnScales = new int[rsColCount];
                  int[] columnPrecisions = new int[rsColCount];
                  int[] columnTypes = new int[rsColCount];
                  String[] columnNames = new String[rsColCount];
                  for ( i = 0; i < rsColCount; i++ )
                  {
                     columnNames[i] = meta.getColumnName(i + 1);
                     columnWidths[i] = meta.getColumnDisplaySize(i + 1);
                     columnTypes[i] = meta.getColumnType(i + 1);
                     columnScales[i] = meta.getScale(i + 1);
                     columnPrecisions[i] = meta.getPrecision(i + 1);
                     if ( columnNames[i].length() > columnWidths[i] )
                        columnWidths[i] = columnNames[i].length(); 
                     lineOut.append(padString(columnNames[i],columnWidths[i]) + " ");
                  }
                  if ( debug ) System.out.println(lineOut.toString());
                  displayResults(display_rs);
               } else if ( cmdString.toUpperCase().startsWith("CONNECT") ) {
                  con = dbConnect(cmdString.substring(8,cmdString.length()));
               } else if ( cmdString.toUpperCase().startsWith("HELP") ) {
                  helpMsg(cmdString);
               } else if ( cmdString.toUpperCase().startsWith("DESCRIBE") ) {
                  tableName = cmdString.toUpperCase().substring(9);
                  display_rs = stmt.executeQuery("SELECT * FROM " + tableName);
                  meta = display_rs.getMetaData();
                  rsColCount = meta.getColumnCount();
                  for ( i = 0; i < rsColCount; i++ )
                  {
                     lineOut = new StringBuffer(100);
                     lineOut.append(padString(meta.getColumnName(i + 1),32));
                     colTypeName = meta.getColumnTypeName(i + 1);
                     colType = meta.getColumnType(i + 1);
                     colWidth = meta.getColumnDisplaySize(i + 1);
                     colScale = meta.getScale(i + 1);
                     colPrecision = meta.getPrecision(i + 1);
                     if ( colTypeName.equals("CHAR") )
                     {
                        colTypeName = colTypeName + "(" 
                        + Integer.toString(colWidth) + ")";
                     } else if ( colTypeName.equals("FLOAT") ) {
                        colTypeName += "("+ Integer.toString(colPrecision)
                        + "," + Integer.toString(colScale) + ")";
                     }  
                     lineOut.append(padString(colTypeName,20) + padString(colType,12));
                     System.out.println(lineOut.toString());
                  }
               } else if ( cmdString.toUpperCase().equals("SHOW TABLES") ) {
                  for ( i = 0; i < tableList.size(); i++ )
                     System.out.println((String)tableList.elementAt(i));
               } else if ( cmdString.toUpperCase().equals("SHOW TYPES") ) {
                  typesRS = dbMeta.getTypeInfo();
                  typeCount = displayResults(typesRS);
               } else if ( cmdString.toUpperCase().startsWith("START ") ) {
                  ft = new FieldTokenizer(cmdString,' ',false);
                  fName = ft.getField(1);
                  if ( !fName.endsWith(".SQL") ) fName += ".SQL";
                  try
                  {
                     startReader = new BufferedReader(new FileReader(fName));
                  } catch ( Exception ex ) {
                     startReader = (BufferedReader)null;
                     throw new tinySQLException("No such file: " + fName);
                  }
               } else if ( cmdString.toUpperCase().startsWith("LOAD") ) {
                  ft = new FieldTokenizer(cmdString,' ',false);
                  fName = ft.getField(1);
                  tableName = ft.getField(3);
                  display_rs = stmt.executeQuery("SELECT * FROM " + tableName);
                  meta = display_rs.getMetaData();
                  rsColCount = meta.getColumnCount();
/*
 *                Set up the PreparedStatement for the inserts
 */
                  prepareBuffer = new StringBuffer("INSERT INTO " + tableName);
                  valuesBuffer = new StringBuffer(" VALUES");
                  for ( i = 0; i < rsColCount; i++ )
                  {
                     if ( i == 0 )
                     {
                        prepareBuffer.append(" (");
                        valuesBuffer.append(" (");
                     } else {
                        prepareBuffer.append(",");
                        valuesBuffer.append(",");
                     }
                     prepareBuffer.append(meta.getColumnName(i + 1));
                     valuesBuffer.append("?");
                  }
                  prepareBuffer.append(")" + valuesBuffer.toString() + ")");
                  try
                  {
                     pstmt = con.prepareStatement(prepareBuffer.toString());
                     loadFileReader = new BufferedReader(new FileReader(fName));
                     while ( (loadString=loadFileReader.readLine()) != null ) 
                     {
                        if ( loadString.toUpperCase().equals("ENDOFDATA") )
                           break;
                        columnIndex = 0;
                        valueIndex = 0;
                        ft = new FieldTokenizer(loadString,'|',true);
                        while ( ft.hasMoreFields() )
                        {
                           fieldString = ft.nextField();
                           if ( fieldString.equals("|") )
                           {
                              columnIndex++;
                              if ( columnIndex > valueIndex )
                              {
                                 pstmt.setString(valueIndex+1,(String)null); 
                                 valueIndex++;
                              }
                           } else if ( columnIndex < rsColCount ) { 
                              pstmt.setString(valueIndex+1,fieldString);
                              valueIndex++;
                           }
                        }
                        pstmt.executeUpdate();
                     }
                     pstmt.close();
                  } catch (Exception loadEx) {
                     System.out.println(loadEx.getMessage());
                  }
               } else if ( cmdString.toUpperCase().startsWith("SETSTRING") |
                           cmdString.toUpperCase().startsWith("SETINT") ) {
                  b1 = cmdString.indexOf(" ");
                  b2 = cmdString.lastIndexOf(" ");
                  if ( b2 > b1 & b1 > 0 )
                  {
                     parameterIndex = Integer.parseInt(cmdString.substring(b1+1,b2));
                     parameterString = cmdString.substring(b2+1);
                     if ( debug ) System.out.println("Set parameter["
                      + parameterIndex + "]=" + parameterString);
                     if ( cmdString.toUpperCase().startsWith("SETINT") )
                     {
                        parameterInt = Integer.parseInt(parameterString);
                        pstmt.setInt(parameterIndex,parameterInt);
                     } else {
                        pstmt.setString(parameterIndex,parameterString);
                     }
                     if ( parameterIndex == 2 ) 
                        pstmt.executeUpdate();
                  }
               } else {
                  if ( cmdString.indexOf("?") > -1 )
                  {
                     pstmt = con.prepareStatement(cmdString);
                  } else {
                     try
                     {
                        stmt.executeUpdate(cmdString);
                        System.out.println("DONE\n");
                     } catch( Exception upex ) {
                        System.out.println(upex.getMessage());
                        if ( debug ) upex.printStackTrace();
                     }
                  }
               }
            }
            if ( args.length > 1 )  cmdString = "EXIT";
         } catch ( SQLException te ) {
              System.out.println(te.getMessage());
              if ( debug ) te.printStackTrace(System.out);
              inputString = (String)null;
         } catch( Exception e ) {
            System.out.println(e.getMessage());
            cmdString = "EXIT";
            break;
         }
      }
   }
   private static void helpMsg(String inputCmd)
   {
      String upperCmd;
      upperCmd = inputCmd.toUpperCase().trim();
      if ( upperCmd.equals("HELP") )
      {
         System.out.println("The following help topics are available:\n"
         + "=============================================================\n"
         + "HELP NEW - list of new features in tinySQL " + dbVersion + "\n"
         + "HELP COMMANDS - help for the non-SQL commands\n"
         + "HELP LIMITATIONS - limitations of tinySQL " + dbVersion + "\n"
         + "HELP ABOUT - short description of tinySQL.\n");
      } else if ( upperCmd.equals("HELP COMMANDS") ) {
         System.out.println("The following non-SQL commands are supported:\n"
         + "=============================================================\n"
         + "SHOW TABLES - lists the tinySQL tables (DBF files) in the current "
         + "directory\n"
         + "SHOW TYPES - lists column types supported by tinySQL.\n"
         + "DESCRIBE table_name - describes the columns in table table_name.\n"
         + "CONNECT directory - connects to a different directory;\n"
         + "   Examples:  CONNECT C:\\TEMP in Windows\n"
         + "              CONNECT /home/mydir/temp in Linux/Unix\n"
         + "EXIT - leave the tinySQL command line interface.\n");
      } else if ( upperCmd.equals("HELP LIMITATIONS") ) {
         System.out.println("tinySQL " + dbVersion 
         + " does NOT support the following:\n"
         + "=============================================================\n"
         + "Subqueries: eg SELECT COL1 from TABLE1 where COL2 in (SELECT ...\n"
         + "IN specification within a WHERE clause.\n"
         + "GROUP BY clause in SELECT statments.\n"
         + "AS in CREATE statements; eg CREATE TABLE TAB2 AS SELECT ...\n"
         + "UPDATE statements including JOINS.\n\n"
         + "If you run into others let us know by visiting\n"
         + "http://sourceforge.net/projects/tinysql\n");
      } else if ( upperCmd.equals("HELP NEW") ) {
         System.out.println("New features in tinySQL " + dbVersion
         + " include the following:\n"
         + "=============================================================\n"
         + "The package name has been changed to com.sqlmagic.tinysql.\n"
         + "Support for table aliases in JOINS: see example below\n"
         + "  SELECT A.COL1,B.COL2 FROM TABLE1 A,TABLE2 B WHERE A.COL3=B.COL3\n"
         + "COUNT,MAX,MIN,SUM aggregate functions.\n"
         + "CONCAT,UPPER,SUBSTR in-line functions for strings.\n"
         + "SYSDATE - current date.\n"
         + "START script_file.sql - executes SQL commands in file.\n"
         + "Support for selection of constants: see example below:\n"
         + "  SELECT 'Full Name: ',first_name,last_name from person\n"
         + "All comparisions work properly: < > = != LIKE \n");
      } else if ( upperCmd.equals("HELP ABOUT") ) {
         System.out.println(
           "=============================================================\n"
         + "tinySQL was originally written by Brian Jepson\n"
         + "as part of the research he did while writing the book \n"
         + "Java Database Programming (John Wiley, 1996).  The database was\n"
         + "enhanced by Andreas Kraft, Thomas Morgner, Edson Alves Pereira,\n"
         + "and Marcel Ruff between 1997 and 2002.\n"
         + "The current version " + dbVersion
         + " was developed by Davis Swan in 2004.\n\n"
         + "tinySQL is free software; you can redistribute it and/or\n"
         + "modify it under the terms of the GNU Lesser General Public\n"
         + "License as published by the Free Software Foundation; either\n"
         + "version 2.1 of the License, or (at your option) any later version.\n"
         + "This library is distributed in the hope that it will be useful,\n"
         + "but WITHOUT ANY WARRANTY; without even the implied warranty of\n"
         + "MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU\n"
         + "Lesser General Public License for more details at\n"
         + "http://www.gnu.org/licenses/lgpl.html");
      } else {
         System.out.println("Unknown help command.\n");
      }
   }     
   private static String padString(int inputint, int padLength)
   {
      return padString(Integer.toString(inputint),padLength);
   }
   private static String padString(String inputString, int padLength)
   {
      String outputString;
      String blanks = "                                        ";
      if ( inputString == (String)null )
         outputString = blanks + blanks + blanks;
      else
         outputString = inputString;
      if ( outputString.length() > padLength )
         return outputString.substring(0,padLength);
      else
         outputString = outputString + blanks + blanks + blanks;
         return outputString.substring(0,padLength);
   }
   private static Connection dbConnect(String tinySQLDir) throws SQLException
   {
      Connection con=null;
      DatabaseMetaData dbMeta;
      File conPath;
      File[] fileList;
      String tableName;
      ResultSet tables_rs;
      conPath = new File(tinySQLDir);
      fileList = conPath.listFiles();
      if ( fileList == null )
      {
         System.out.println(tinySQLDir + " is not a valid directory.");
         return (Connection)null;
      } else {
         System.out.println("Connecting to " + conPath.getAbsolutePath());
         con = DriverManager.getConnection("jdbc:dbfFile:" + conPath, "", "");
      }
      dbMeta = con.getMetaData();
      tables_rs = dbMeta.getTables(null,null,null,null);
      tableList = new Vector();
      while ( tables_rs.next() )
      {
         tableName = tables_rs.getString("TABLE_NAME");
         tableList.addElement(tableName);
      }
      if ( tableList.size() == 0 )
         System.out.println("There are no tinySQL tables in this directory.");
      else
         System.out.println("There are " + tableList.size() + " tinySQL tables"
         + " in this directory.");
      return con;
   }
  /**
  Formatted output to stdout
  @return number of tuples
  */
  static int displayResults(ResultSet rs) throws java.sql.SQLException
  {
    if (rs == null) {
      System.err.println("ERROR in displayResult(): No data in ResulSet");
      return 0;
    }

    int numCols = 0;

    ResultSetMetaData meta = rs.getMetaData();
    int cols = meta.getColumnCount();
    int[] width = new int[cols];
    String dashes = "=============================================";

    // To Display column headers
    //
    boolean first=true;
    StringBuffer head = new StringBuffer();
    StringBuffer line = new StringBuffer();

    // fetch each row
    //
    while (rs.next()) {

      // get the column, and see if it matches our expectations
      //
      String text = new String();
      for (int ii=0; ii<cols; ii++) {
        String value = rs.getString(ii+1);
        if (first) {
          width[ii] = meta.getColumnDisplaySize(ii+1);
          if (meta.getColumnName(ii+1).length() > width[ii])
            width[ii] = meta.getColumnName(ii+1).length();
          head.append(padString(meta.getColumnName(ii+1), width[ii]));
          head.append(" ");
          line.append(padString(dashes+dashes,width[ii]));
          line.append(" ");
        }
        text += padString(value, width[ii]);
        text += " ";   // the gap between the columns
      }
      if (first) {
        System.out.println(head.toString());
        System.out.println(line.toString());
        first = false;
      }
      System.out.println(text);
      numCols++;
    }
    return numCols;
  }
}
