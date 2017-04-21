package com.faimdata.dataloader.reader;

import com.faimdata.dataloader.content.Content;
import com.faimdata.dataloader.database.Connector;
import com.faimdata.dataloader.database.PushData;
import com.faimdata.dataloader.singleton.PropertiesFactory;
import com.faimdata.dataloader.util.Utils;

import java.io.BufferedReader;

/**
 * Created by Bin Zhu on 3/19/2017.
 */
public class FileReader
{
    private String fileName = "";
    private BufferedReader br = null;
    private String line = "";
    private Content content;


    public FileReader(String fileName)
    {
        this.fileName = fileName;
    }

    public Content process() throws Exception
    {
        long t1 = System.currentTimeMillis();

        String[] columns = null;
        int totalCount = 0;
        int contentCount = 0;
        int badFormatCount = 0;
        int badDataTypeCount = 0;
        int goodCount = 0;
        content = new Content();

        try
        {
            br = new BufferedReader(new java.io.FileReader(fileName));

            while ((line = br.readLine()) != null)
            {
                totalCount ++;

                //Skip the header lines
                if(totalCount < PropertiesFactory.input.getFirstLine())
                {
                    continue;
                }
                contentCount ++;

                columns = line.split(PropertiesFactory.input.getSeparator());
               
                if(columns != null && columns.length > 0)
                {
                    if(!Utils.validateLine(columns, PropertiesFactory.input.getInputSchema(), PropertiesFactory.input.getDateFormat()))
                    {
                        badDataTypeCount ++;
                        printLine("Bad DataType -", totalCount, columns);
                    }
                    else
                    {
                        content.appendLine(columns);
                        goodCount ++;
                        //Write data
                        
                        if (PropertiesFactory.output.getBatchSize()>0&&goodCount%PropertiesFactory.output.getBatchSize()==0){
                        Connector conn = new Connector();
                        PushData pd = new PushData();
                        pd.setConnection(conn.getConnection());
                        pd.setContent(content);
                        pd.push();
                        conn.closeConnection();
                        content.empty();
                        }
                    }
                }
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            br.close();
        }
        //Print summary
        double timeCost = ((Number)(System.currentTimeMillis() - t1)).doubleValue()/1000;

        System.out.println("Processed the File:" + this.fileName + " in " + timeCost + " Seconds");
        System.out.println("Total Records:" + totalCount);
        System.out.println("Total Content Records:" + contentCount);
        System.out.println("Total Iegal Records:" + goodCount);
        System.out.println("Bad Format Lines:" + badFormatCount);
        System.out.println("Bad DataType Lines:" + badDataTypeCount);
        System.out.println("--------------------");

        return content;
    }

    private void printLine(String note,int lineNumber, String[] line)
    {
        if(line != null && line.length > 0)
        {
            for(int i = 0; i < line.length; i ++)
            {
                if(PropertiesFactory.input.getInputSchema().size() > i)
                {
                    System.out.println(note + " Line:" + lineNumber + " -- Column " + PropertiesFactory.input.getInputSchema().get(i+1).toString() + ":" + line[i]);
                }
                else
                {
                    System.out.println(note + " Line:" + lineNumber + " -- Column Index:[" + (i+1) + "]" + ":" + line[i]);
                }
            }
            System.out.println("-----------------------------");
        }
    }
}