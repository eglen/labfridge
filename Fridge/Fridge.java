import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

/**
 * This code was edited or generated using CloudGarden's Jigloo SWT/Swing GUI
 * Builder, which is free for non-commercial use. If Jigloo is being used
 * commercially (ie, by a corporation, company or business for any purpose
 * whatever) then you should purchase a license for each developer using Jigloo.
 * Please visit www.cloudgarden.com for details. Use of Jigloo implies
 * acceptance of these licensing terms. A COMMERCIAL LICENSE HAS NOT BEEN
 * PURCHASED FOR THIS MACHINE, SO JIGLOO OR THIS CODE CANNOT BE USED LEGALLY FOR
 * ANY CORPORATE OR COMMERCIAL PURPOSE.
 */
public class Fridge extends javax.swing.JFrame implements ActionListener, KeyListener {
	private JPanel jPanel1;

	private JTextField consoleInputField;

	private JTextArea consoleDisplay;

	private JScrollPane consoleScrollPane;

	private JLabel instructionsLabel;
	
	private Vector tableList;
	
	private Statement stmt = null;
	
	//The same transaction is recycled for all transactions
	private Transaction  transaction = null;

	{
		// Set Look & Feel
		try {
			javax.swing.UIManager
					.setLookAndFeel("com.sun.java.swing.plaf.gtk.GTKLookAndFeel");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Auto-generated main method to display this JFrame
	 */
	public static void main(String[] args) {
		Fridge inst = new Fridge();
		inst.setVisible(true);
		
	}

	public Fridge() {
		super();
		initGUI();
		//Setup Listeners
		consoleInputField.addActionListener(this);
		consoleInputField.addKeyListener(this);
		//setup db connection
		try {
			stmt = getDbStatement();
			transaction = new Transaction(stmt,consoleDisplay);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	private Statement getDbStatement() throws SQLException {
		/*
		 * Register the JDBC driver for dBase
		 */
		try {
			Class.forName("com.sqlmagic.tinysql.dbfFileDriver");
		} catch (ClassNotFoundException e) {
			System.err.println("JDBC Driver could not be registered!!\n");
		}
		String fName = ".";
		/*
		 * Establish a connection to dBase
		 */
		Connection con = dbConnect(fName);

		// Print out the current list of tabs
		Statement stmt = con.createStatement();
		return stmt;
	}
	
	private Connection dbConnect(String tinySQLDir) throws SQLException
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

	private void initGUI() {
		Font instructionsFont = new Font("Arial", Font.PLAIN, 36);
		Font consoleFont = new Font("Arial", Font.PLAIN, 24);
		Font inputFont = new Font("Arial", Font.PLAIN, 24);
		try {
			{
				jPanel1 = new JPanel();
				BoxLayout mainBoxLayout = new BoxLayout(jPanel1,
						javax.swing.BoxLayout.Y_AXIS);
				getContentPane().add(jPanel1, BorderLayout.CENTER);
				jPanel1.setLayout(mainBoxLayout);
				{
					instructionsLabel = new JLabel();
					jPanel1.add(instructionsLabel);
					instructionsLabel.setText("Scan Items then Student Card");
					// instructionsLabel.setPreferredSize(new
					// java.awt.Dimension(80, 50));
					instructionsLabel.setFont(instructionsFont);
					instructionsLabel.setForeground(Color.RED);
				}
				{
					consoleScrollPane = new JScrollPane();
					jPanel1.add(consoleScrollPane);
					consoleScrollPane.setPreferredSize(new java.awt.Dimension(
							392, 193));
					{
						consoleDisplay = new JTextArea();
						consoleScrollPane.setViewportView(consoleDisplay);
						// consoleDisplay.setText("Items:");
						consoleDisplay.setFont(consoleFont);
						consoleDisplay.setEditable(false);
					}
				}
				{
					consoleInputField = new JTextField();
					jPanel1.add(consoleInputField);
					// consoleInputField.setText("inputField");
					consoleInputField.setMaximumSize(new java.awt.Dimension(
							5000, 50));
					consoleInputField.setPreferredSize(new java.awt.Dimension(
							541, 50));
					consoleInputField.setFont(inputFont);
				}
			}
			this.setSize(549, 481);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void actionPerformed(ActionEvent arg0) {
		if(consoleInputField.getText().length()
				> 0)
		{
		//System.out.println("Enter pressed - this could be an unknown code?");
		Product product = new Product(consoleInputField.getText(),stmt);
		User user = new User(consoleInputField.getText(),stmt);
		if(product.getName() != null && user.getName() == null)
		{
			//Product has been selected, add it to the current transaction (or start a transaction)
			transaction.add(product);
			consoleInputField.setText("");
		}
		else if (product.getName() == null && user.getName() != null)
		{
			//User has been selected.. tell the transaction
			transaction.setUser(user);
			consoleInputField.setText("");
		}
		else if (product.getName()!= null && user.getName() != null)
		{
			//This is a significant problem.. should have an error message here
		}
		else
		{
			//The user specifically asked us to record this.. must be a new user or product
			String input = consoleInputField.getText();
			if(input.length() == 14)
			{
				//14 digits.. assuming it's a new user (*Here's hoping no UPCs have 14 digits*) :)
				String newName =  JOptionPane.showInputDialog("Please enter your Name");
				String newEmail =  JOptionPane.showInputDialog("Please enter your Email Address");
				User newUser = new User(input,newName,newEmail,stmt);
				transaction.setUser(newUser);
				consoleInputField.setText("");
				//System.out.println("Made new user");
			}
			else
			{
				//We assume this is a product.
				String newName = JOptionPane.showInputDialog("Please enter the name of this product");
				String newCost = JOptionPane.showInputDialog("Please enter the value of " + newName, "-75");
				Product newProduct = new Product(input,newName,Integer.parseInt(newCost),stmt);
				transaction.add(newProduct);
				consoleInputField.setText("");
				//System.out.println("Made new product");
			}
		}
		}
		//System.out.flush();
	}

	public void keyTyped(KeyEvent arg0) {
				
	}

	public void keyPressed(KeyEvent arg0) {
		
	}

	public void keyReleased(KeyEvent arg0) {
		System.out.println("Checking: " + consoleInputField.getText());
		//Check as they type.. have to see how bad this is with the scanner
		
		Product product = new Product(consoleInputField.getText(),stmt);
		User user = new User(consoleInputField.getText(),stmt);
		if(product.getName() != null && user.getName() == null)
		{
			//Product has been selected, add it to the current transaction (or start a transaction)
			transaction.add(product);
			consoleInputField.setText("");
		}
		else if (product.getName() == null && user.getName() != null)
		{
			//User has been selected.. tell the transaction
			transaction.setUser(user);
			consoleInputField.setText("");
		}
		else if (product.getName()!= null && user.getName() != null)
		{
			//This is a significant problem.. should have an error message here
		}
		else
		{
			//Nothing special.. just let things keep going
			try {
				//Let the rest of the letters come in before we bother trying again
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//System.out.println("Nothing found with that ID.. waiting");
		}
		System.out.flush();
	}

}
