package com.faimdata.dataloader.obj;

/**
 * Created by Bin Zhu on 3/19/2017.
 */
public class InputSchema
{
    private int index = 0;
    private String header = "";
    private String dataType = "";
    private String dateFormat = "";

    public int getIndex()
    {
        return index;
    }

    public void setIndex(int index)
    {
        this.index = index;
    }

    public String getHeader()
    {
        return header;
    }

    public void setHeader(String header)
    {
        this.header = header;
    }

    public String getDataType()
    {
        return dataType;
    }

    public void setDataType(String dataType)
    {
        this.dataType = dataType;
    }

    public String getDateFormat()
    {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat)
    {
        this.dateFormat = dateFormat;
    }

    //Overwrite
    public String toString()
    {
        if(this.dateFormat != null && dateFormat.length() > 0)
        {
            return "Index:[" + this.getIndex() + "] Column Name: [" + this.getHeader() + "] DataType: [" + this.getDataType() + "] DateFormat: [" + this.getDateFormat() + "]";
        }
        else
        {
            return "Index:[" + this.getIndex() + "] Column Name: [" + this.getHeader() + "] DataType: [" + this.getDataType() + "]";
        }
    }
}
