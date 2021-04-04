package com.lisz;

import java.sql.*;

public class HiveJDBCClient {
	private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
	private static final String URL = "jdbc:hive2://hadoop-04:10000/default";
	private static final String USERNAME = "root";
	private static final String PASSWORD = "^abc123$";


	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(DRIVER_NAME);  // 执行静态语句块注册org.apache.hive.jdbc.HiveDriver
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
		PreparedStatement pstmt = connection.prepareStatement("select * from psn4");
		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			System.out.println(rs.getString(1) + "-" + rs.getString("name") + "-" + rs.getObject("likes") + "-" + rs.getObject(4));
		}
		connection.close();
	}
}
