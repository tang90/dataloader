package com.faimdata.dataloader.obj;

import java.util.Map;

/**
 * Created by b_zhu on 3/20/2017.
 */
public class Input {
    private String separator;
    private int columnNumber = 0;
    private Map<Integer, InputSchema> inputSchema;
    private String dateFormat;
    private int firstLine = 0;

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public int getColumnNumber() {
        return columnNumber;
    }

    public void setColumnNumber(int columnNumber) {
        this.columnNumber = columnNumber;
    }

    public Map<Integer, InputSchema> getInputSchema() {
        return inputSchema;
    }

    public void setInputSchema(Map<Integer, InputSchema> inputSchema) {
        this.inputSchema = inputSchema;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public int getFirstLine()
    {
        return firstLine;
    }

    public void setFirstLine(int firstLine)
    {
        this.firstLine = firstLine;
    }
}
