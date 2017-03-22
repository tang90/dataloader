package com.faimdata.dataloader.util;

import com.faimdata.dataloader.datatype.DataType;
import com.faimdata.dataloader.obj.InputSchema;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by b_zhu on 3/19/2017.
 */
public class Utils
{
    public static boolean validateValue(String value, String dataType, String dateType)
    {
        if(dataType.toUpperCase().equals(DataType.String.getDataType().toUpperCase()))
        {
            return true;
        }

        if(dataType.toUpperCase().equals(DataType.Integer.getDataType().toUpperCase()))
        {
            try
            {
                Integer.parseInt(value);
                return true;
            }
            catch (Exception e)
            {
                System.out.println("The value [" + value + "] of dataType [" + dateType + "] is invalidated"  );
            }
        }

        if(dataType.toUpperCase().equals(DataType.Double.getDataType().toUpperCase()))
        {
            try
            {
                if(value.substring(0,1).equals("$"))
                {
                    Double.parseDouble(value.substring(1));
                    return true;
                }
                else
                {
                    Double.parseDouble(value);
                    return true;
                }
            }
            catch (Exception e)
            {
                System.out.println("The value [" + value + "] of dataType [" + dateType + "] is invalidated"  );
            }
        }

        if(dataType.toUpperCase().equals(DataType.Boolean.getDataType().toUpperCase()))
        {
            try
            {
                Boolean.parseBoolean(value);
                return true;
            }
            catch (Exception e)
            {
                System.out.println("The value [" + value + "] of dataType [" + dateType + "] is invalidated"  );
            }
        }

        if(dataType.toUpperCase().equals(DataType.Date.getDataType().toUpperCase()))
        {
            if(dateType == null || dateType.length() == 0)
            {
                return false;
            }

            SimpleDateFormat formatter = new SimpleDateFormat(dateType);

            try
            {
                formatter.parse(value);
                return true;
            }
            catch (Exception e)
            {
                System.out.println("The value [" + value + "] of dataType [" + dateType + "] is invalidated"  );
                e.printStackTrace();
            }
        }
        return false;
    }

    public static String getDefaultValue(String dataType)
    {
        if(dataType.toUpperCase().equals(DataType.String.getDataType().toUpperCase()))
        {
            return DataType.String.getDefaultValue();
        }

        if(dataType.toUpperCase().equals(DataType.Integer.getDataType().toUpperCase()))
        {
            return  DataType.Integer.getDefaultValue();
        }

        if(dataType.toUpperCase().equals(DataType.Double.getDataType().toUpperCase()))
        {
            return DataType.Double.getDefaultValue();
        }

        if(dataType.toUpperCase().equals(DataType.Boolean.getDataType().toUpperCase()))
        {
            return DataType.Boolean.getDefaultValue();
        }

        if(dataType.toUpperCase().equals(DataType.Date.getDataType().toUpperCase()))
        {
            return DataType.Date.getDefaultValue();
        }
        return "";
    }


    public static Map<Integer, InputSchema> parseInput(String input)
    {
        if(input == null || input.length() == 0)
        {
            return null;
        }

        String[] items = input.split(",");

        if(items == null || items.length == 0)
        {
            return null;
        }

        String index = "";
        String column = "";
        String dataType = "";
        String[] cd = null;
        InputSchema schema;
        Map<Integer, InputSchema> result = new HashMap<Integer, InputSchema>();

        for(String item : items)
        {
            if(item != null && item.length() > 0 && item.contains(":"))
            {
                cd = item.split(":");

                if(cd != null && cd.length == 3)
                {
                    schema = new InputSchema();
                    schema.setIndex(Integer.parseInt(cd[0]));
                    schema.setHeader(cd[1]);
                    schema.setDataType(cd[2]);

                    if(!result.containsKey(index))
                    {
                        result.put(schema.getIndex(), schema);
                    }
                }
            }
        }
        return result;
    }

    public static boolean validateLine(String[] line, Map<Integer, InputSchema> schema, String dateType)
    {
        if(line == null || line.length == 0)
        {
            return false;
        }
        String col;
        for (int i = 0; i < line.length; i ++)
        {
            if(!validateValue(line[i], schema.get(new Integer(i+1)).getDataType(), schema.get(new Integer(i+1)).getDateFormat()))
            {
                return false;
            }
        }
        return true;
    }
}
