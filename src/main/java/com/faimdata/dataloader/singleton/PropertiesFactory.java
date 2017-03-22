package com.faimdata.dataloader.singleton;

import com.faimdata.dataloader.obj.DBInfo;
import com.faimdata.dataloader.obj.Input;
import com.faimdata.dataloader.obj.InputSchema;
import com.faimdata.dataloader.obj.Output;
import com.faimdata.dataloader.util.Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;

/**
 * Created by Bin Zhu on 3/20/2017.
 */
public class PropertiesFactory
{
    private Properties prop;
    private InputStream inputStream;
    private String propertiesFile = "";

    public static Input input;
    public static DBInfo dbInfo;
    public static Output output;

    public PropertiesFactory(String propertiesFile)
    {
        this.propertiesFile = propertiesFile;

        try
        {
            init();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.out.println("The dataLoader can't initialize the configuration and is quiting...");
            System.exit(1);
        }
    }

    private void init() throws Exception
    {
        prop = new Properties();
        File file = new File(this.propertiesFile);
        inputStream = new FileInputStream(file);
        prop.load(inputStream);

        if(prop != null)
        {
            System.out.println("Initializing the Configuration variables...");
            input = new Input();
            input.setColumnNumber(Integer.parseInt(prop.getProperty("number_of_column")));
            input.setDateFormat(prop.getProperty("date_format"));
            input.setSeparator(prop.getProperty("separator"));
            input.setInputSchema(Utils.parseInput(prop.getProperty("input")));

            //init dateformat
            if(prop.getProperty("date_format") != null && prop.getProperty("date_format").length() > 0)
            {
                String[] date_format_array = prop.getProperty("date_format").split(",");
                String data_format;
                String[] date_format_index;
                if (date_format_array != null && date_format_array.length > 0)
                {
                    for(String s : date_format_array)
                    {
                        date_format_index = s.split("&");

                        if(date_format_index != null && date_format_index.length == 2)
                        {
                            InputSchema schema = PropertiesFactory.input.getInputSchema().get(Integer.parseInt(date_format_index[0]));
                            schema.setDateFormat(date_format_index[1]);
                        }
                    }

                }
            }

            input.setFirstLine(Integer.parseInt(prop.getProperty("first_line")));

            System.out.println("-----Input Configuration------");
            System.out.println("[number_of_column]=" + input.getColumnNumber());
            System.out.println("[date_format]=" + input.getDateFormat());
            System.out.println("[separator]=" + input.getSeparator());
            System.out.println("[first_line]=" + input.getFirstLine());
            System.out.println("[Input Format] ------------------");
            Iterator it = input.getInputSchema().keySet().iterator();
            Integer index;
            while (it.hasNext())
            {
                index = (Integer)it.next();
                System.out.println(input.getInputSchema().get(index).toString());
            }
            System.out.println("[Input Format] ------------------");

            dbInfo = new DBInfo();
            dbInfo.setUrl(prop.getProperty("database"));
            dbInfo.setDbName(prop.getProperty("dbuser"));
            dbInfo.setPassword(prop.getProperty("dbpassword"));

            System.out.println("-----Database Configuration------");
            System.out.println("[database]=" + dbInfo.getUrl());
            System.out.println("[dbuser]=" + dbInfo.getDbName());
            System.out.println("[dbpassword]=" + dbInfo.getPassword());

            output = new Output();
            output.setSql(prop.getProperty("sql"));
            output.setOutputColumns(prop.getProperty("output_column"));
            output.setBatchSize(Integer.parseInt(prop.getProperty("transaction_batch_size")));

            System.out.println("-----Output Configuration------");
            System.out.println("[sql]=" + output.getSql());
            System.out.println("[output_column]=" + output.getOutputColumns());
            System.out.println("[transaction_batch_size]=" + output.getBatchSize());
            System.out.println("-------------------------------");


        }
        else
        {
            System.out.println("The dataLoader can't initialize the configuration and is quiting...");
            System.exit(1);
        }

    }



}
