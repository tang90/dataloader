package com.faimdata.dataloader;

import com.faimdata.dataloader.content.Content;
import com.faimdata.dataloader.database.Connector;
import com.faimdata.dataloader.database.PushData;
import com.faimdata.dataloader.reader.FileReader;
import com.faimdata.dataloader.singleton.PropertiesFactory;

/**
 * Created by Bin Zhu on 3/19/2017.
 */
public class EntryPoint
{

    public static void  main(String[] args)
    {
        //args[0] configuration file
        //args[1] input file
        if(args != null && args.length != 2)
        {
            System.out.println("Please pass the Properties file path and input file!");
            System.out.println("Example: java -jar dataLoader.jar properties.conf email.csv");
            return;
        }

        String propertiesFile = args[0];
        String inputFile = args[1];

        new PropertiesFactory(propertiesFile);

        Content content = null;
        try
        {
            //Read data from file
            FileReader reader = new FileReader(inputFile);
            content = reader.process();

            //Write data
            Connector conn = new Connector();
            PushData pd = new PushData();
            pd.setConnection(conn.getConnection());
            pd.setContent(content);
            pd.push();
            conn.closeConnection();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

    }
}
