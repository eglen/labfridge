import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;

import javax.swing.JTextArea;

public class Transaction {

	//The database statement to use for completing transactions
	Statement stmt;
	
	//The area to write all the transaction info to.
	JTextArea output;
	
	//The list of products in the transaction
	ArrayList products = new ArrayList();
	
	//Number formatter
	DecimalFormat df;
	
	public Transaction(Statement stmt, JTextArea consoleDisplay) {
		this.stmt = stmt;
		this.output = consoleDisplay;
		df = new DecimalFormat('\u00A4'+"####.00");
	}

	public void add(Product product) {
		if(products.size() == 0)
		{
			//Clear out the old receipt
			output.setText("");
		}
		products.add(product);	
		//update the receipt
		output.append(product.getName() + '\t' + df.format(product.getCost()/100.0) + '\n');
	}

	public void setUser(User user) {
		//If there are products in the list, use this user to pay for them.
		if(products.size() > 0)
		{
			int total = 0;
			//update all the products with their new count.. and keep track of the costs.
			for (int i = 0; i < products.size(); i++) {
				Product curProduct = (Product)products.get(i);
				total += curProduct.getCost();
				try {
					stmt.executeQuery("UPDATE PRODUCTS SET COUNT='" + (curProduct.getCount() + 1) + "' WHERE ID = '" + curProduct.getPId() + "'");
					
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			//Gone through all products.. update the users tab
			try {
				user.setTab(user.getTab()+total);
				stmt.executeQuery("UPDATE USERS SET TAB='" + user.getTab() + "' WHERE ID = '" + user.getUserId() + "'");
				output.append("==========" + '\n');
				output.append("Total this transaction: " + df.format(total/100.0) + '\n');
				output.append("Your tab: " + df.format(user.getTab()/100.0) + '\n');
				output.append("Thanks, " + user.getName() + '\n');
				output.append("*Please keep a positive tab to" + '\n' + "*help us maintain product selection." + '\n');
				new CountdownThread(output).start();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else
		{
			//If not, print out their tab
			output.append("User: " + user.getName() + '\n');
			output.append("Tab: " + df.format(user.getTab()/100.0) + '\n');
			output.append("*Please keep a positive tab to" + '\n' + "*help us maintain product selection." + '\n');
			new CountdownThread(output).start();
		}
		
	}

}
