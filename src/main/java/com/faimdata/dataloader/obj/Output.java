package com.faimdata.dataloader.obj;

/**
 * Created by Bin Zhu on 3/20/2017.
 */
public class Output
{
    private String sql;
    private String outputColumns;
    private int batchSize;

    public String getSql()
    {
        return sql;
    }

    public void setSql(String sql)
    {
        this.sql = sql;
    }

    public String getOutputColumns()
    {
        return outputColumns;
    }

    public void setOutputColumns(String outputColumns)
    {
        this.outputColumns = outputColumns;
    }

    public int getBatchSize()
    {
        return batchSize;
    }

    public void setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
    }
}
