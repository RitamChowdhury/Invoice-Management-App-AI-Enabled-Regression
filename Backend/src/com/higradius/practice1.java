package com.higradius;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

public class practice1 {
	public static void main(String args[]) throws Exception {
//Registering the Driver
		
		Class.forName("com.mysql.cj.jdbc.Driver");
//Getting the connection
		String mysqlUrl = "jdbc:mysql://localhost:3306/h2h_internship";
		
		
		Connection con = DriverManager.getConnection(mysqlUrl, "root", "qwerty");
//Creating a Statement object
		Statement stmt = con.createStatement();
//Retrieving the data
		ResultSet rs = stmt.executeQuery("select * from superheroes ");

//Creating an ArrayList object
		ArrayList<Superhero> studList = new ArrayList<Superhero>();
//Adding the Records of the table to the Array List
		while (rs.next()) {
			Superhero studObj = new Superhero();
			
			studObj.setFname(rs.getString("first_name"));
			studObj.setLname(rs.getString("last_name"));
			studObj.setSerial(rs.getInt("serial"));
			studObj.setAlias(rs.getString("alias"));
			studObj.setQuotes(rs.getString("quotes"));
			studList.add(studObj);
		}
		for (Superhero obj : studList) {
			
			System.out.println("First_Name: " + obj.getFname() + ", ");
			System.out.println("Last_Name: " + obj.getLname() + ", ");
			System.out.println("Serial: " + obj.getSerial() + ", ");
			System.out.println("Alias: " + obj.getAlias() + ", ");
			System.out.println("Quotes: " + obj.getQuotes() );
			System.out.println("-----------");
		}
	}
}