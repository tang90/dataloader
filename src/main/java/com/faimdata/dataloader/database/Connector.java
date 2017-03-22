package com.faimdata.dataloader.database;

import com.faimdata.dataloader.singleton.PropertiesFactory;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by Bin Zhu on 3/20/2017.
 */
public class Connector
{
    private String url;
    private String user;
    private String password;
    private Connection connection = null;

    public Connector()
    {
        try
        {
            Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException e)
        {
             e.printStackTrace();
            return;
        }

        System.out.println("PostgreSQL JDBC Driver Registered!");

        this.url = PropertiesFactory.dbInfo.getUrl();
        this.user = PropertiesFactory.dbInfo.getDbName();
        this.password = PropertiesFactory.dbInfo.getPassword();
    }

    public Connection getConnection()
    {

        try
        {
            connection = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to " + PropertiesFactory.dbInfo.getUrl());

        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
        return connection;
    }

    public void closeConnection() throws Exception
    {
        connection.close();
        connection = null;
    }
}
