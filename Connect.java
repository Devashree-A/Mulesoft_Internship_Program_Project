package net.sqlitetutorial;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement; 
import java.sql.ResultSet;  
import java.sql.Statement;  
import java.sql.SQLException;
import java.sql.*;

public class Connect {
	
	String url;
	
	public static String createNewDatabase(String fileName)
	{
		String url = "jdbc:sqlite:C:/sqlite/" + fileName;
		
		try
		{
			Connection conn = DriverManager.getConnection(url);  
            if (conn != null) {  
                DatabaseMetaData meta = conn.getMetaData();  
                System.out.println("The driver name is " + meta.getDriverName());  
                System.out.println("A new database has been created.");
            }  
   
        } catch (SQLException e) {  
            System.out.println(e.getMessage());  
        } 
		return url;
    }  
	
	  public static void createNewTable(String url) {
		  System.out.println("Inside createNewTBLE");
	        String sql = "CREATE TABLE IF NOT EXISTS Movies (\n"
	                + " Movie_Name text NOT NULL,\n"  
	                + " Actor text,\n"  
	                + " Actress text,\n"
	                + " Year_Release integer,\n"
	                + " Director_Name text\n"
	                + ");";  
	          
	        try{  
	            Connection conn = DriverManager.getConnection(url);  
	            Statement stmt = conn.createStatement(); 
	           
	            stmt.execute(sql); 
	            System.out.println("Table created");
	        } catch (SQLException e) {  
	            System.out.println(e.getMessage());  
	        }  
	    }  
	  
	  public void insert(String Movie_Name, String Actor, String Actress, int Year, String Director) {  
	        String sql = "INSERT INTO Movies VALUES(?,?,?,?,?)";  
	   
	        try{  
	            Connection conn = DriverManager.getConnection(this.url); 
	            PreparedStatement pstmt = conn.prepareStatement(sql);  
	            pstmt.setString(1, Movie_Name);  
	            pstmt.setString(2, Actor);
	            pstmt.setString(3, Actress);
	            pstmt.setInt(4, Year);
	            pstmt.setString(5, Director);
	            pstmt.executeUpdate();  
	        } catch (SQLException e) {  
	            System.out.println(e.getMessage());  
	        }  
	    }  
	
 
	  public void selectAll(){  
	        String sql = "SELECT * FROM Movies";  
	          
	        try {  
	            Connection conn = DriverManager.getConnection(this.url);   
	            Statement stmt  = conn.createStatement();  
	            ResultSet rs    = stmt.executeQuery(sql);  
	               
	            System.out.println("-----------------------------------------------------------------------------------------------------------  \n");
	           while (rs.next()) {  
	                System.out.println("Movie Name: " + rs.getString("Movie_Name") + "   " + 
         		           "Actor: " + rs.getString("Actor") + "   " + 
         		           "Actress: " + rs.getString("Actress") + "   " + 
         				   "Year Released: " + rs.getInt("Year_Release") +  "   " + 
                            "Director: " + rs.getString("Director_Name") ); 
	                
	                System.out.println("-----------------------------------------------------------------------------------------------------------  \n");
	                
	            }  
	        } catch (SQLException e) {  
	            System.out.println(e.getMessage());  
	        }  
	    }  
	  
	  public void conditionalSelect(String Director){  
	        String sql = "SELECT * FROM Movies WHERE Director_Name=? ";  
	          
	        try {  
	            Connection conn = DriverManager.getConnection(this.url);   
	            PreparedStatement pstmt = conn.prepareStatement(sql);  
	            pstmt.setString(1, Director);
	            ResultSet rs = pstmt.executeQuery(); 
	                        
	            System.out.println("List of Movies directed by " + Director + " is: \n");
	            while (rs.next()) {  
	                System.out.println("Movie Name: " + rs.getString("Movie_Name") + "   " + 
	                		           "Actor: " + rs.getString("Actor") + "   " + 
	                		           "Actress: " + rs.getString("Actress") + "   " + 
	                				   "Year Released: " + rs.getInt("Year_Release") +  "   " + 
	                                   "Director: " + rs.getString("Director_Name") ); 
	               
	            }  
	        } catch (SQLException e) {  
	            System.out.println(e.getMessage());  
	        }  
	    } 
	  
    public static void main(String[] args) {  
    	
    	String url;
    	Connect conObj = new Connect();
    	
        url = conObj.createNewDatabase("Movie_List.db");  
        conObj.url = url;
        conObj.createNewTable(url);
        
        conObj.insert("Captain Marvel", "Samuel L. Jackson", "Brie Larson", 2019, "Anna Boden");
        conObj.insert("Spider-Man: Far From Home", "Tom Holland", "Zendaya", 2019, "Jon Watts");
        conObj.insert("Captain America: The First Avenger","Chris Evans","Hayley Atwell",2011,"Joe Johnston");
        conObj.insert("Mr. And Mrs. Smith","Brad Pitt","Angelina Jolie",2005,"Doug Liman");
        conObj.insert("Avatar","Sam Worthington","Zoe Saldana",2009,"James Cameron");
        conObj.insert("Titanic","Leonardo Di Caprio","Kate Winslet",1997,"James Cameron");
        conObj.insert("Maleficent","Sam Riley","Angelina Jolie",2014,"Robert Stromberg");
        conObj.insert("3 Idiots","Aamir Khan","Kareena Kapoor",2009,"Rajkumar Hirani");
        conObj.insert("Taare Zameen Par","Aamir Khan","Tisca Chopra",2007,"Aamir Khan");
        conObj.insert("The Conjuring","Patrick Wilson","Vera Farmiga",2013,"James Wan");
        
        conObj.selectAll();
          
        conObj.conditionalSelect("James Cameron");
    }
}