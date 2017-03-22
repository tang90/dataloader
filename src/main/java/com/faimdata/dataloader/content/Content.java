package com.faimdata.dataloader.content;

import com.faimdata.dataloader.singleton.PropertiesFactory;
import com.faimdata.dataloader.util.Utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Bin Zhu on 3/20/2017.
 */
public class Content
{
    private List<List<String>> content;

    public Content()
    {
        content = new ArrayList<List<String>>();
    }

    public void appendLine(String[] oldLine)
    {
        if(oldLine != null && oldLine.length > 0)
        {
            List<String> line = new ArrayList<String>();

            for(int i = 0; i < PropertiesFactory.input.getColumnNumber(); i ++)
            {
                if(i < oldLine.length)
                {
                    line.add(oldLine[i]);
                }
                else
                {
                    //Append the default value to the missing columns
                    //Get the data type and retrieve the default value of this data type
                    line.add(Utils.getDefaultValue(PropertiesFactory.input.getInputSchema().get(i+1).getDataType()));
                }
            }
            content.add(line);
        }
    }

    public int getRecords()
    {
        return this.content.size();
    }

    public List<String> getLine(int line)
    {
        return this.content.get(line);
    }
}
