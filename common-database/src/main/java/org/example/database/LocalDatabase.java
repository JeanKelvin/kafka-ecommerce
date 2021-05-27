package org.example.database;

import java.sql.*;

public class LocalDatabase {

    private final Connection connection;

    public LocalDatabase(String host, String name) throws SQLException {
        String url = "jdbc:sqlite:./"+ host + "/target/" + name + ".db";
        this.connection = DriverManager.getConnection(url);
    }

    // yes, this is way too generic
    // according to your database tool, avoid injection
    public void createIfNotExists(String sql) {
        try {
            this.connection.createStatement().execute(sql);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public void update(String statement, String... params) throws SQLException {
        prepare(statement, params).execute();
    }

    public ResultSet query(String query, String... params) throws SQLException {
        return prepare(query, params).executeQuery();
    }

    private PreparedStatement prepare(String query, String[] params) throws SQLException {
        var prepareStatement = this.connection.prepareStatement(query);
        for (int i = 0; i < params.length; i++) {
            prepareStatement.setString(i + 1, params[i]);
        }
        return prepareStatement;
    }

    public void close() throws SQLException {
        connection.close();
    }
}
