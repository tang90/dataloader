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
            pstmt.setDouble(number, Double.parseDouble(value));
            return;
        }

        if(dataType.toUpperCase().equals(DataType.Boolean.getDataType().toUpperCase()))
        {
            pstmt.setBoolean(number, Boolean.parseBoolean(value));
            return;
        }

        if(dataType.toUpperCase().equals(DataType.Date.getDataType().toUpperCase()))
        {
            formatter = new SimpleDateFormat(dateFormat);
            Date date = formatter.parse(value);
            pstmt.setTimestamp(number, new java.sql.Timestamp(date.getTime()));
            return;
        }
    }
}
