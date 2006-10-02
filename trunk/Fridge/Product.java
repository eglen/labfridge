import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Product {

	int cost;
	int count;
	String name = null;
	String pId;
	
	public Product(String pId, Statement stmt) {
		//Do a query to build the product
		try {
			ResultSet rs = stmt.executeQuery("SELECT * from PRODUCTS WHERE ID = '" + pId + "'");
			//Assuming all IDs are unique
			rs.next();
			if(rs.getString("NAME") != null)
			{
				//Valid product
				cost = rs.getInt("COST");
				count = rs.getInt("COUNT");
				this.pId = pId;
				this.name = rs.getString("NAME");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Product(String input, String newName, int newCost, Statement stmt) {
		this.pId = input;
		this.name = newName;
		this.cost = newCost;
		this.count = 0;
		try {
			stmt.executeQuery("INSERT INTO PRODUCTS (ID, NAME, COST, COUNT) VALUES ('" + pId + "','" + name + "','" + cost + "','" + count +"'");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public String getName() {
		return name;
	}

	public int getCost() {
		return cost;
	}
	
	public int getCount() {
		return count;
	}
	
	public String getPId() {
		return pId;
	}

}
