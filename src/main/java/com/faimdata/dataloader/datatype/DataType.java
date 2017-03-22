package com.faimdata.dataloader.datatype;

/**
 * Created by Bin Zhu on 3/20/2017.
 */
public enum DataType
{
    String ("string", ""),
    Date ("date", ""),
    Integer ("integer", "0"),
    Double ("double", "0.0"),
    Boolean ("boolean", "false");

    private String dataType;
    private String defaultValue;

    DataType(String dataType, String defaultValue)
    {
        this.dataType = dataType;
        this.defaultValue = defaultValue;
    }

    public String getDataType()
    {
        return this.dataType;
    }

    public String getDefaultValue()
    {
        return this.defaultValue;
    }
}
