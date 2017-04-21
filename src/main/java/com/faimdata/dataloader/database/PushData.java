package com.faimdata.dataloader.database;

import com.faimdata.dataloader.content.Content;
import com.faimdata.dataloader.datatype.DataType;
import com.faimdata.dataloader.singleton.PropertiesFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by Bin Zhu on 3/20/2017.
 */
public class PushData
{
    private Content content;
    private Connection connection;
    private String[] columns;
    private String sql;
    private int columnNumber;
    private DateFormat formatter;

    public PushData()
    {
        columns = PropertiesFactory.output.getOutputColumns().split(",");
        if(columns != null && columns.length > 0)
        {
            this.columnNumber = columns.length;
        }
        sql = PropertiesFactory.output.getSql();
    }

    public void setContent(Content content)
    {
        this.content = content;
    }

    public void setConnection(Connection connection)
    {
        this.connection = connection;
    }

    public void push() throws Exception
    {
        int insertCount = 0;
        int internalCount = 0;
        connection.setAutoCommit(false);
        PreparedStatement pstmt = connection.prepareStatement(this.sql);

        int columnIndex = 0;
        String dataType = "";
        String value = "";
        List<String> line;
        double timeCost;

        System.out.println("--------------");
        System.out.println("dataLoader will push " + content.getRecords() + " records into database...");
        long start = System.currentTimeMillis();
        for (int lineNumber = 0; lineNumber < content.getRecords(); lineNumber ++)
        {
            line = content.getLine(lineNumber);

            for(int i = 0; i < this.columnNumber; i ++)
            {
                columnIndex = Integer.parseInt(this.columns[i]);
                dataType = PropertiesFactory.input.getInputSchema().get(columnIndex).getDataType();
                value = line.get(columnIndex-1);

                insertData(pstmt, (i + 1), dataType, value, PropertiesFactory.input.getInputSchema().get(columnIndex).getDateFormat());
            }
            pstmt.executeUpdate();
            insertCount ++;
            internalCount ++;

            if(PropertiesFactory.output.getBatchSize() > 0 && internalCount == PropertiesFactory.output.getBatchSize())
            {
                connection.commit();
                System.out.println("Committed " + internalCount + " records.");
                internalCount = 0;
            }

            if(insertCount%1000 == 0)
            {
                timeCost = ((Number)(System.currentTimeMillis() - start)).doubleValue()/1000;
                System.out.println("Inserted " + insertCount + " records in " + timeCost + " seconds");
            }

        }
        connection.commit();
        System.out.println("Committed transaction");

        timeCost = ((Number)(System.currentTimeMillis() - start)).doubleValue()/1000;
        System.out.println("Inserted " + insertCount + " records in " + timeCost + " seconds");
    }

    private void insertData(PreparedStatement pstmt, int number, String dataType, String value, String dateFormat) throws Exception
    {
        if(dataType.toUpperCase().equals(DataType.String.getDataType().toUpperCase()))
        {
            pstmt.setString(number, value);
            return;
        }

        if(dataType.toUpperCase().equals(DataType.Integer.getDataType().toUpperCase()))
        {
            pstmt.setInt(number, Integer.parseInt(value));
            return;
        }

        if(dataType.toUpperCase().equals(DataType.Double.getDataType().toUpperCase()))
        {
            String v=new String(value);
            double x=1;
            if (v.startsWith("("))
            {
            	v=v.substring(1,v.length()-1);
            	x=-1;
            }
            if (v.startsWith("$"))
            	v=v.substring(1);
            pstmt.setDouble(number, x*Double.parseDouble(v));
            return;
        }

        if(dataType.toUpperCase().equals(DataType.Boolean.getDataType().toUpperCase()))
        {
            pstmt.setBoolean(number, Boolean.parseBoolean(value));
            return;
        }

        if(dataType.toUpperCase().equals(DataType.Date.getDataType().toUpperCase()))
        {
            String v=new String(value);
           /* if (dateFormat.equals("MM/dd/yyyy"))
            {
            	String[] ds=v.split("/");
            	if (ds[0].length()<2)
            		for (int i=ds[0].length();i<=2;i++)
            			ds[0]="0"+ds[0];
            	if (ds[1].length()<2)
            		for (int i=ds[1].length();i<=2;i++)
            			ds[1]="0"+ds[1];
            	if (ds[2].length()<4)
            		for (int i=ds[2].length();i<=4;i++)
            			ds[2]="0"+ds[2];
            	v=ds[0]+"/"+ds[1]+"/"+ds[2];
            }
            */
            
        	formatter = new SimpleDateFormat(dateFormat);
            Date date = formatter.parse(v);
            pstmt.setTimestamp(number, new java.sql.Timestamp(date.getTime()));
            return;
        }
    }
}
