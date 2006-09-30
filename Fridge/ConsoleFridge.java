import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

/**
 * 
 */

/**
 * @author eglen
 *
 *create table USERS (id char(14), name char(255), email char(255))
 */
public class ConsoleFridge {

	static Vector tableList;
	/**
	 * @param args
	 * @throws SQLException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws SQLException, IOException {
		
		//The maximum length of codes we expect to see
		int maxCodeLength = 20;
		
		/*
		 *       Register the JDBC driver for dBase
		 */
		try{
		         Class.forName("com.sqlmagic.tinysql.dbfFileDriver");
		      } catch (ClassNotFoundException e) {
		         System.err.println(
		              "JDBC Driver could not be registered!!\n");
		      }
		      String fName = ".";
		      /* 
		       *    Establish a connection to dBase
		       */
		      Connection con = dbConnect(fName);
				
				//Print out the current list of tabs
				Statement stmt = con.createStatement();
				ResultSet rs = null;
				rs = stmt.executeQuery("SELECT * from USERS");
				displayResults(rs);
				
		//Infinite loop looking for input
		while(true)
		{
			//Ask for card
			for(int i=0;i<35;i++)
			{
				System.out.println("");
			}
			System.out.print("Scan Items:");
			
			//For console input
			InputStreamReader isr = new InputStreamReader(System.in);
			BufferedReader br = new BufferedReader(isr);

			//Loop until we have a code we recognize or enter is hit
			boolean valid = false;
			//To store the read
			char[] cbuf = new char[maxCodeLength];
			int bufferFill = 0;
			while(!valid)
			{
				int readInt = System.in.read();
				
				System.out.println("read: " + readInt);
				/*if(isr.ready())
				{
					System.out.println("Ready?: " + isr.ready());
					System.out.println("Read: " + isr.read());
					bufferFill = 0;
				}
				else
				{
					System.out.println("Ready?: " + isr.ready());
					try {
						Thread.sleep(500);
						bufferFill++;
						if(bufferFill > 50)
						{
							System.out.println("Exiting on timeout");
							System.exit(0);
						}
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}*/
				
				//Only read the remainder of the buffer
				/*br.read(cbuf,bufferFill,1);
				bufferFill++;
				//Now see if what's in the buffer makes sense
				System.out.print("Buffer fill:");
				for (int i = 0; i < cbuf.length; i++) {
					System.out.print(cbuf[i]);
				}
				System.out.println(" Done");
				*/
				
			}
			
		      //  read the username from the command-line; need to use try/catch with the
		      //  readLine() method
			//br.r
		      String userId = br.readLine();
		      if(userId.length() == 14)
		      {
		    	  	//Has the right number of digits see if they're in the db
					rs = stmt.executeQuery("SELECT * from USERS WHERE Id='" + userId +"'");
					rs.next();
					if(rs.getString("NAME") == null)
					{
						//this person doesn't already exist.. make an account for them
						System.out.println("I don't have you on record.");
						System.out.print("Please Enter your name:");
						String name = br.readLine();
					     System.out.print("Please Enter your email address (we'll use this if we move the fridge or prices change):");
							String email =  br.readLine();
						//Put their data into the database
						stmt.executeQuery("INSERT INTO USERS (ID, NAME, EMAIL, TAB) VALUES ('" + userId + "','" + name + "','" + email +"','" + 0 + "')");
						rs = stmt.executeQuery("SELECT * from USERS WHERE Id='" + userId +"'");
						rs.next();
						System.out.println("User " + rs.getString("NAME") + " added" + '\n');
					}
					if(rs.getString("NAME") != null)
					{
						//must be some data here,
						System.out.println("Hello " + rs.getString("NAME"));
						int tab = rs.getInt("TAB");
						System.out.println("Your current tab is: " + (double)tab/100);	
						System.out.println("Please scan the top of the fridge to add money to your account");
						System.out.println("Scan your student card to quit" + '\n');
						//Now keep scanning items while decrementing their tab
						String pId = br.readLine();
						while (pId.length() != 14)
						{
							//See if this product exists in the database
							rs = stmt.executeQuery("SELECT * from PRODUCTS WHERE ID = '" + pId + "'");
							rs.next();
							if(rs.getString("NAME") == null)
							{
								//Product does not exist
								System.out.println("This product does not exist in the database.");
								System.out.print("Please enter the name of the product:");
								String pName = br.readLine();
								stmt.executeQuery("INSERT INTO PRODUCTS (ID, NAME, COST, COUNT) VALUES ('" + pId + "','" + pName + "','-75','0')");
								rs = stmt.executeQuery("SELECT * from PRODUCTS WHERE ID = '" + pId + "'");
								rs.next();
							}
							if(rs.getString("NAME") != null)
							{
								//Found a product
								String productName = rs.getString("NAME");
								int cost = rs.getInt("COST");
								int count = rs.getInt("COUNT") + 1;
								tab += cost;
								System.out.println(productName + '\t' + (double)tab/100);
								//Update the database
								stmt.executeQuery("UPDATE USERS SET TAB='" + tab + "' WHERE ID = '" + userId +"'");
								
								stmt.executeQuery("UPDATE PRODUCTS SET COUNT='" + count + "' WHERE ID = '" + pId + "'");
							}
						    pId = br.readLine();
						}
					}
		      }
		      else
		      {
		    	  System.out.println("Invalid Student Card I think.  If I'm wrong talk to Ed or email him: eglen@cs.sfu.ca");
		      }
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
