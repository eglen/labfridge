/*
 * tsResultSet.java - Result Set object for tinySQL.
 * 
 * Copyright 1996, Brian C. Jepson
 *                 (bjepson@ids.net)
 * $Author: davis $
 * $Date: 2004/12/18 21:26:18 $
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
 */

package com.sqlmagic.tinysql;

import java.util.*;
import java.lang.*;
import java.io.*;
import java.sql.*;

/*
 *
 * tsResultSet - object to hold query results
 *
 * @author Thomas Morgner <mgs@sherito.org> Changed tsResultSet to support java.sql.Types,
 * a custom fetchsize, and storing a state for unfinished queries.
 * I also marked all members as private and use access-Functions to set or query
 * this ResultSets properties.
 */
public class tsResultSet {

   private Vector rows;// = new Vector();    // all the rows
   private Vector tsColumns;// = new Vector(); // all the tsColumn objects
   private Vector columns; // SQL-Query columns
 	private Vector tables; // SQL-Query information
   private tinySQLWhere whereC;
 
   private int fetchsize;
   private int windowStart;
   private int level;
   private tinySQL dbengine;
   private Hashtable sTables;
   private Vector groupByColumns;
   private Vector orderByColumns;
   private String orderType;
   private int type;
   private boolean eof;
   private boolean groupedColumns = false;
   public String newLine = System.getProperty("line.separator");
   
   public tsResultSet (Vector t, Vector c, tinySQLWhere w, tinySQL dbeng)
   {
      dbengine = dbeng;
      windowStart = 0;
      tables = t;
      columns = c;
      whereC = w;
      rows = new Vector ();
      tsColumns = new Vector ();
   }
   public void setState (int pstate, Hashtable ptables, Vector inputGroupBy,
                         Vector inputOrderBy,String inputOrderType)
   {
      sTables = ptables;
      groupByColumns = inputGroupBy;
      orderByColumns = inputOrderBy;
      orderType = inputOrderType;
      level = pstate;
   }
   public void setType (int type)
   {
      if ((type == ResultSet.TYPE_FORWARD_ONLY) ||
        (type == ResultSet.TYPE_SCROLL_SENSITIVE) ||
        (type == ResultSet.TYPE_SCROLL_INSENSITIVE))
     
      this.type = type;
   }
   public int getType ()
   {
      return type;
   }
   public void setFetchSize (int i)
   {
      fetchsize = i;
   }
   public int getFetchSize ()
   {
      return fetchsize;
   }
   public void addColumn (tsColumn col)
   {
      tsColumns.addElement (col);
      if ( col.isGroupedColumn() ) groupedColumns = true;
   }
   public boolean isGrouped()
   {
      return groupedColumns;
   }
   public boolean getMoreResults (int newPos, int fetchsize)
   {
      this.fetchsize = fetchsize;
      if (dbengine != null)
      {
         try
     	 {
            if (type != ResultSet.TYPE_SCROLL_INSENSITIVE)
            {
               rows.removeAllElements();
               windowStart = newPos;
            }
	    dbengine.contSelectStatement (this); 
   	    if (level != 0)
            {
               eof = false;
	       return eof;
            }
         } catch (tinySQLException e) {
         }
      }
      eof = true;
      return eof;
   }
   public boolean addRow (tsRow row)
   {
      return addRow(row,"ASC");
   }
   public boolean addRow (tsRow row,String orderType)
   {
      int i;
      boolean sortUp=true;
      tsRow sortRow;
      if ( orderType != (String)null ) 
         if ( orderType.startsWith("DESC") ) sortUp = false;
      if ( rows.size() > 0 ) 
      {
/*
 *       Insert or append the row depending upon the ORDER BY
 *       conditions and a comparison of the new and the existing row.
 */
         if ( sortUp )
         {
            for ( i = rows.size() -1; i > -1; i-- )
            {
               sortRow = (tsRow)rows.elementAt(i);
               if ( row.compareTo(sortRow) < 0 ) continue;
               if ( i == rows.size() - 1 )
                  rows.addElement(row);
               else 
                  rows.insertElementAt(row,i+1);
               return true;
            }
         } else {
            for ( i = rows.size() -1; i > -1; i-- )
            {
               sortRow = (tsRow)rows.elementAt(i);
               if ( row.compareTo(sortRow) > 0 ) continue;
               if ( i == rows.size() - 1 )
                  rows.addElement(row);
               else 
                  rows.insertElementAt(row,i+1);
               return true;
            }
         }
         rows.insertElementAt(row,0);
         return true;
      }
      rows.addElement (row);
      if ((fetchsize > 0) && (rows.size () >= fetchsize)) return false;
      return true;   
   }
/*
 * The following methods update a particular row in the result set.
 */
   public void updateRow(tsRow inputRow)
   {
      updateRow(inputRow,rows.size()-1);
   }
   public void updateRow(tsRow inputRow, int rowIndex)
   {
      rows.setElementAt(inputRow,rowIndex);
   }
   public Vector getTables ()
   {
      return tables;
   }
   public int getLevel ()
   {
      return level;
   }
   public void setLevel (int l)
   {
      level = l;
   }
   public Hashtable getTableState ()
   {
      return sTables;
   }
   public Vector getGroupByColumns ()
   {
      return groupByColumns;
   }
   public Vector getOrderByColumns ()
   {
      return orderByColumns;
   }
   public String getOrderType()
   {
      return orderType;
   }
   public tinySQLWhere getWhereClause ()
   {
      return whereC;
   }
/*
 * Returns the number of columns in the result set.
 */
   public int numcols() 
   {
      return tsColumns.size();
   }
/*
 * Update all the columns in the ResultSet.
 */
   public void updateColumns(String inputColumnName,String inputColumnValue)
      throws tinySQLException
   {
      int i;
      tsColumn tcol;
      for ( i = 0; i < tsColumns.size(); i++ )
      {
         tcol = (tsColumn)tsColumns.elementAt(i);
         tcol.update(inputColumnName,inputColumnValue);
      }
   }
/*
 * Returns the number of rows in the result set.
 */
   public int size() 
   {
      return rows.size();
   }
/*
 * Returns the tsRow at a given row offset (starts with zero).
 *
 * @param i the row offset/index
 */
   public tsRow rowAt(int row)
   {
      int i;
      if (row >= (windowStart + rows.size()))
      {
      	 getMoreResults (row, fetchsize);
      }
      i = row - windowStart;
      if ( i < rows.size () )
      {
         return (tsRow) rows.elementAt(i);
      }
      return null;
   }
/*
 * Returns the tsColumn at a given column offset (starts with zero).
 *
 * @param i the column offset/index
 */
   public tsColumn columnAtIndex (int i) 
   {
      return (tsColumn) tsColumns.elementAt(i);
   }
/* 
 * Debugging method to dump out the result set
 */
   public String toString()
   {
      int i;
      tsColumn tcol;
      StringBuffer outputBuffer;
/*
 *    Display columns
 */
      outputBuffer = new StringBuffer(newLine + "Columns in ResultSet"
      + newLine);
      for ( i = 0; i < tsColumns.size(); i++ )
      {
         tcol = (tsColumn)tsColumns.elementAt(i);
         outputBuffer.append(tcol.toString());
      }
      outputBuffer.append(newLine + "Rows in tsResultSet" + newLine);
      for (i = 0; i < size(); i++) 
      {
         tsRow row = rowAt(i);
         outputBuffer.append(row.toString() + newLine);
      }
      return outputBuffer.toString();
   }
}
