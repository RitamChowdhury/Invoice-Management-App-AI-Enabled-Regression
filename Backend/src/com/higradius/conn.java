package com.higradius;
import java.sql.*;
public class conn {
 public static void main(String []args) {
  Connection dbCon = null;
  PreparedStatement pstmt = null;
  //Statement stmt = null;
  ResultSet rs = null;
  String url = "jdbc:mysql://localhost:3306/h2h_internship";
  String user = "root";
  String pass = "qwerty";
  String query = "select * from superheroes ";
  try {
   Class.forName("com.mysql.cj.jdbc.Driver");
   dbCon = DriverManager.getConnection(url, user, pass);
   if(dbCon!=null)
    System.out.println("Connection successful");
   else
    System.out.println("Connection unsuccessful");
   pstmt = dbCon.prepareStatement(query);
   rs = pstmt.executeQuery();
  // while(rs.next()) {
    //String t = rs.getString("title");
    //if(t.length()>12)
     //System.out.println(t);
   //}
   dbCon.close();
   pstmt.close();
   rs.close();   
  }
  catch(SQLException s) {
   s.printStackTrace();
  }
  catch(Exception e) {
   e.printStackTrace();
  }  
 }
}