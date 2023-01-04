package com.sindhujamicrocredit.etl;

import java.sql.Connection;
import java.sql.DriverManager;

public class DB {

    public static Connection getConnection(String url, String user, String pass) throws Exception{
        Connection con = null;
        con = DriverManager.getConnection(url, user, pass);
        return con;
    }
}
