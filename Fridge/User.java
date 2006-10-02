import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class User {
	
	String userId;
	String name = null;
	String email;
	int tab;
	
	public User(String userId, Statement stmt) {
		//Do a query to build the user
		try {
			ResultSet rs = stmt.executeQuery("SELECT * from USERS WHERE ID = '" + userId + "'");
			//Assuming all IDs are unique
			rs.next();
			if(rs.getString("NAME") != null)
			{
				//Valid product
				this.tab = rs.getInt("TAB");
				this.name = rs.getString("NAME");
				this.userId = userId;
				this.email = rs.getString("EMAIL");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * For a completely new user that we have to add to the database
	 */
	public User(String input, String newName, String newEmail,Statement stmt) {
		this.userId = input;
		this.name = newName;
		this.email = newEmail;
		this.tab = 0;
		try {
			stmt.executeQuery("INSERT INTO USERS (ID, NAME, EMAIL, TAB) VALUES ('" + userId + "','" + name + "','" + email +"','" + tab + "')");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String getEmail() {
		return email;
	}

	public String getName() {
		return name;
	}

	public int getTab() {
		return tab;
	}
	
	public void setTab(int newTab){
		this.tab = newTab;
	}

	public String getUserId() {
		return userId;
	}
	
}
