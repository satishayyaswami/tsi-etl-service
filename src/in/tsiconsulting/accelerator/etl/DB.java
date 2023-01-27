package in.tsiconsulting.accelerator.etl;

import org.apache.commons.lang3.StringEscapeUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;

public class DB {

    public static Connection getConnection(String url, String user, String pass) throws Exception{
        Connection con = null;
        con = DriverManager.getConnection(url, user, pass);
        return con;
    }

    public static JSONObject getResultsWithHeader(ResultSet rs) {
        JSONObject jsonResult = new JSONObject();
        try {
            if (rs != null) {
                ResultSetMetaData rsmd = rs.getMetaData();
                JSONArray header = new JSONArray();
                for (int i = 1, colCount = rsmd.getColumnCount(); i <= colCount; i++) {
                    header.add(rsmd.getColumnLabel(i));
                }
                JSONArray jsonResultArr = new JSONArray();
                while (rs.next()) {
                    JSONObject json = getJSON(rs);
                    jsonResultArr.add(json);
                }
                jsonResult.put("header", header);
                jsonResult.put("data", jsonResultArr);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return jsonResult;
    }

    public static String getResultAsHTML(ResultSet rs) {
        StringBuffer buff = new StringBuffer();
        JSONObject jsonResult = new JSONObject();
        buff.append(("<table>"));
        try {
            if (rs != null) {
                ResultSetMetaData rsmd = rs.getMetaData();
                JSONArray header = new JSONArray();
                buff.append(("<tr>"));
                for (int i = 1, colCount = rsmd.getColumnCount(); i <= colCount; i++) {
                    buff.append(("<td><b>"));
                    buff.append(rsmd.getColumnLabel(i));
                    buff.append(("</td></b>"));
                }
                buff.append(("</tr>"));
                JSONArray jsonResultArr = new JSONArray();
                while (rs.next()) {
                    buff.append(("<tr>"));
                    buff.append(getHTMLRow(rs));
                    buff.append(("</tr>"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        buff.append(("</table>"));
        return buff.toString();
    }

    private static String getHTMLRow(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        StringBuffer buff = new StringBuffer();
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
        SimpleDateFormat sdfTimeStamp = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss a");
        for (int index = 1, colCount = rsmd.getColumnCount(); index <= colCount; index++) {
            String value = null;
            switch (rsmd.getColumnType(index)) {
                case Types.VARCHAR:
                case Types.CHAR: {
                    value = rs.getString(index);
                    if (value == null) value = "";
                }
                break;

                case Types.TIMESTAMP: {
                    Timestamp timeStamp = rs.getTimestamp(index);
                    String sTimeStamp = "";
                    if (timeStamp != null) {
                        sTimeStamp = sdfTimeStamp.format(new Date(timeStamp.getTime()));
                    }
                    value = sTimeStamp;
                }
                break;

                case Types.DATE: {
                    Date date = rs.getDate(index);
                    if (date != null) {
                        value = sdf.format(date);
                    }
                }
                break;

                case Types.BIGINT:
                case Types.NUMERIC: {
                    value = rs.getLong(index)+"";
                }
                break;

                case Types.INTEGER: {
                    value = rs.getInt(index)+"";
                }
                break;

                case Types.FLOAT: {
                    value = new BigDecimal(rs.getFloat(index)).toPlainString();
                }
                break;

                case Types.DECIMAL: {
                    value = new BigDecimal(rs.getDouble(index)).toPlainString();
                }
                break;

                case Types.DOUBLE: {
                    value = new BigDecimal(rs.getDouble(index)).toPlainString();
                }
                break;

                default: {
                    value = rs.getString(index);
                    if (value == null) value = "";
                }
            }
            buff.append("<td>");
            buff.append(StringEscapeUtils.escapeHtml4(value));
            buff.append("</td>");
        }
        return buff.toString();
    }

    private static JSONObject getJSON(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        JSONObject json = new JSONObject();
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
        SimpleDateFormat sdfTimeStamp = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss a");
        for (int index = 1, colCount = rsmd.getColumnCount(); index <= colCount; index++) {
            Object value = null;
            switch (rsmd.getColumnType(index)) {
                case Types.VARCHAR:
                case Types.CHAR: {
                    value = rs.getString(index);
                    if (value == null) value = "";
                }
                break;

                case Types.TIMESTAMP: {
                    Timestamp timeStamp = rs.getTimestamp(index);
                    String sTimeStamp = "";
                    if (timeStamp != null) {
                        sTimeStamp = sdfTimeStamp.format(new Date(timeStamp.getTime()));
                    }
                    value = sTimeStamp;
                }
                break;

                case Types.DATE: {
                    Date date = rs.getDate(index);
                    if (date != null) {
                        value = sdf.format(date);
                    }
                }
                break;

                case Types.BIGINT:
                case Types.NUMERIC: {
                    value = rs.getLong(index);
                }
                break;

                case Types.INTEGER: {
                    value = rs.getInt(index);
                }
                break;

                case Types.FLOAT: {
                    value = rs.getFloat(index);
                }
                break;

                case Types.DECIMAL: {
                    value = rs.getDouble(index);
                }
                break;

                case Types.DOUBLE: {
                    value = rs.getDouble(index);
                }
                break;

                default: {
                    value = rs.getString(index);
                    if (value == null) value = "";
                }
            }
            json.put(rsmd.getColumnLabel(index), value);
        }
        return json;
    }
}
