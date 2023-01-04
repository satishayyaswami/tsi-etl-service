package com.sindhujamicrocredit.etl;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.*;

import java.sql.*;
import java.util.Date;

public class ETLManager {

    private static int VARCHAR_LENGTH = 99;

    public static void main(String[] argv) throws Exception {
        String confDir = null;
        Config config = null;
        JSONObject jsonConfig = null;

        if(argv.length != 1)
            throw new Exception("Invalid params");

        confDir = argv[0];
        config = new Config(confDir);
        jsonConfig = config.getConfig();
        System.out.println("Preparing Data Dictionary");
        prepareDataDictionary(jsonConfig);
        System.out.println("Loading Stage Tables");
        loadStage(jsonConfig);
        System.out.println("Loading Operational Data Store");
        loadODS(jsonConfig);
        System.out.println("Creating Datamarts");
        loadDatamarts(jsonConfig);
    }

    private static void loadDatamarts(JSONObject jsonConfig) throws Exception{
        Connection con = null;
        Statement stmt = null;
        JSONArray tasks = null;
        Iterator taskIt = null;
        JSONObject task = null;
        String tablename = null;
        String dmschema = null;
        String dmImplStrategy = null;
        String dmSQL = null;

        try {
            con = DB.getConnection( (String) jsonConfig.get("db-url"),
                    (String) jsonConfig.get("db-user"),
                    (String) jsonConfig.get("db-pass"));
            stmt = con.createStatement();
            tasks = (JSONArray) jsonConfig.get("datamarts");
            taskIt = tasks.iterator();
            while(taskIt.hasNext()) {
                task = (JSONObject) taskIt.next();
                tablename = (String) task.get("dm-table-name");
                dmschema = (String) task.get("dm-schema");
                dmImplStrategy = (String) task.get("dm-impl-strategy");
                dmSQL = (String) task.get("dm-sql");
                stmt.executeUpdate("DROP TABLE IF EXISTS "+tablename);
                stmt.executeUpdate(dmschema);
                if(dmImplStrategy.equalsIgnoreCase("SQL")) {
                    stmt.executeUpdate(dmSQL);
                }
            }
        }finally {
            if(stmt != null) stmt.close();
            if(con != null) con.close();
        }
    }

    private static void prepareDataDictionary(JSONObject jsonConfig) throws Exception{
        Connection con = null;
        Statement stmt = null;
        String ddpath = null;
        HashMap<String,List> datadictionay = null;
        Iterator<String> dictionaryIt = null;

        try {
            con = DB.getConnection( (String) jsonConfig.get("db-url"),
                    (String) jsonConfig.get("db-user"),
                    (String) jsonConfig.get("db-pass"));
            stmt = con.createStatement();
            // Create Data Dictionary
            stmt.executeUpdate(createDataDictionarySQL());

            // Load Data Dictionary
            ddpath = jsonConfig.get("data-dictionary-dir")+"/"+"data-dictionary.csv";
            //System.out.println(ddpath);
            stmt.executeUpdate("DELETE FROM _sys_data_dictionary");
            stmt.executeUpdate("LOAD DATA INFILE \"" + ddpath + "\" INTO TABLE "+
                    "_sys_data_dictionary FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' IGNORE 1 LINES");
        }finally {
            if(stmt != null) stmt.close();
            if(con != null) con.close();
        }
    }

    private static void loadStage(JSONObject jsonConfig) throws Exception{
        Connection con = null;
        Statement stmt = null;
        JSONArray tasks = null;
        Iterator taskIt = null;
        JSONObject task = null;
        String path = null;
        String createSQL = null;
        String tablename = null;

        try {
            con = DB.getConnection( (String) jsonConfig.get("db-url"),
                                    (String) jsonConfig.get("db-user"),
                                    (String) jsonConfig.get("db-pass"));
            stmt = con.createStatement();
            tasks = (JSONArray) jsonConfig.get("stage-load-tasks");
            taskIt = tasks.iterator();
            while(taskIt.hasNext()) {
                task = (JSONObject) taskIt.next();
                path = jsonConfig.get("raw-data-dir")+"/"+task.get("file-name");
                tablename = (String) task.get("table-name");
                createSQL = getStageTableCreateSQL(tablename,path);
                //System.out.println(createSQL);
                stmt.executeUpdate("DROP TABLE IF EXISTS "+tablename);
                stmt.executeUpdate(createSQL);
                stmt.executeUpdate("LOAD DATA INFILE \"" + path + "\" INTO TABLE "
                                         + tablename + " FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' IGNORE 1 LINES");
            }
        }finally {
            if(stmt != null) stmt.close();
            if(con != null) con.close();
        }
    }

    private static void loadODS(JSONObject jsonConfig) throws Exception{
        Connection con = null;
        Statement stmt = null;
        String ddpath = null;
        HashMap<String,List> datadictionay = null;
        Iterator<String> dictionaryIt = null;
        ArrayList table = null;
        String tablename = null;
        JSONObject odsLoadTask = null;
        String strategy = null;
        String odsLoadSQL = null;
        HashMap<String,String> exceptions = null;
        FileWriter validatorlog = new FileWriter(jsonConfig.get("logs-dir")+"/"+"validator"+".log",true);

        try {
            con = DB.getConnection( (String) jsonConfig.get("db-url"),
                    (String) jsonConfig.get("db-user"),
                    (String) jsonConfig.get("db-pass"));
            stmt = con.createStatement();
            datadictionay = getDataDictionary(con);

            // Build ODS Objects
            dictionaryIt = datadictionay.keySet().iterator();
            while(dictionaryIt.hasNext()){
                tablename = (String) dictionaryIt.next();
                odsLoadTask = getODSLoadTask(jsonConfig, tablename);
                strategy = (String) odsLoadTask.get("strategy");
                odsLoadSQL = (String) odsLoadTask.get("sql");
                table = (ArrayList) datadictionay.get(tablename);
                if(strategy.equalsIgnoreCase("FULL_LOAD")) {
                    createODSTable(con, table, tablename);
                    stmt.executeUpdate(odsLoadSQL);
                }
                validate(validatorlog, con, table,tablename);
            }
        }finally {
            if(validatorlog != null) validatorlog.close();
            if(stmt != null) stmt.close();
            if(con != null) con.close();
        }
    }

    private static HashMap<String,String> validate(FileWriter validatorlog, Connection con, ArrayList table, String tablename) throws Exception{
        HashMap<String,String> exceptions = new HashMap<String,String>();
        ResultSet rs = null;
        Statement stmt = null;
        Iterator fieldIt = null;
        JSONObject datafield = null;
        String fieldname = null;
        String datatype = null;
        String charlength = null;
        String acceptablevalue = null;
        String fieldrequired = null;
        String fieldvalue = null;
        boolean datatypevalid = false;
        boolean charlengthvalid = false;
        boolean requiredcheckmet = false;

        stmt = con.createStatement();
        rs = stmt.executeQuery("select * from "+tablename+" limit 1");
        while(rs.next()){
            fieldIt = table.iterator();
            while(fieldIt.hasNext()){
                datafield = (JSONObject) fieldIt.next();
                fieldname = (String) datafield.get("field_name");
                datatype = (String) datafield.get("data_type");
                charlength = (String) datafield.get("char_length");
                acceptablevalue = (String) datafield.get("acceptable_value");
                fieldrequired = (String) datafield.get("required");

                fieldvalue = rs.getString(fieldname);
                datatypevalid = isDataTypeValid(fieldvalue, datatype);
                charlengthvalid = isCharLengthValid(fieldvalue, charlength);
                requiredcheckmet = isRequiredCheckMet(fieldvalue, fieldrequired);
                validatorlog.write("Table: "+tablename+" Field: "+fieldname+" Data Type Valid: "+datatypevalid+" Char Length Valid: "+charlengthvalid+" Requirement Check Met: "+requiredcheckmet+"\n");
            }
        }
        return exceptions;
    }

    private static boolean isRequiredCheckMet(String fieldvalue, String fieldrequired) {
        boolean valid = false;
        if(fieldrequired.equalsIgnoreCase("1") && fieldvalue != null && !fieldvalue.equals("") || fieldrequired.equalsIgnoreCase("0"))
            valid = true;
        return valid;
    }

    private static boolean isCharLengthValid(String fieldvalue, String charlength) {
        boolean valid = false;
        int length = fieldvalue.length();
        if(length <= Integer.parseInt(charlength))
            valid = true;

        //System.out.println(fieldvalue+"-"+length+"-"+charlength+"-"+valid);
        return valid;
    }

    private static boolean isDataTypeValid(String fieldvalue, String datatype){
        boolean valid = false;
        int numtest = 0;
        Date datetest = null;

        try {
            if (datatype.equalsIgnoreCase("number")) {
                numtest = Integer.parseInt(fieldvalue);
            }else if(datatype.equalsIgnoreCase("number")){
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                // With lenient parsing, the parser may use heuristics to interpret
                // inputs that do not precisely match this object's format.
                format.setLenient(false);
                format.parse(fieldvalue);
            }
            valid = true;
        }catch(Exception e){
            valid = false;
        }
        return valid;
    }

    private static JSONObject getODSLoadTask(JSONObject config, String tablename){
        JSONObject odsLoadTask = null, jsob = null;
        JSONArray odsLoadTasks = (JSONArray) config.get("ods-load-tasks");
        Iterator taskIt = odsLoadTasks.iterator();
        while(taskIt.hasNext()){
            jsob = (JSONObject) taskIt.next();
            if(((String)jsob.get("table-name")).equalsIgnoreCase(tablename)){
                odsLoadTask = jsob;
                break;
            }
        }
        return odsLoadTask;
    }

    private static String createDataDictionarySQL(){
        StringBuffer buff = new StringBuffer();
        buff.append("CREATE TABLE IF NOT EXISTS _sys_data_dictionary (");
        buff.append("table_name varchar(50),");
        buff.append("field_name varchar(50),");
        buff.append("description varchar(100),");
        buff.append("data_type varchar(25),");
        buff.append("char_length varchar(2),");
        buff.append("acceptable_value varchar(25),");
        buff.append("field_required varchar(2),");
        buff.append("accept_null_value varchar(2)");
        buff.append(");");
        return buff.toString();
    }

    private static void createODSTable(Connection con, ArrayList table, String tablename) throws Exception{
        Statement stmt = null;
        String createsql = null;
        try{
            stmt = con.createStatement();
            stmt.executeUpdate("DROP TABLE IF EXISTS "+tablename);
            createsql = getODSTableCreateSQL(tablename, table);
            stmt.executeUpdate(createsql);
        }finally {
            if(stmt != null) stmt.close();
        }
    }

    private static String getODSTableCreateSQL(String tablename, ArrayList table) throws Exception {
        StringBuffer buff = new StringBuffer();
        Iterator fieldIt = null;
        JSONObject field = null;
        String datatype = null;
        buff.append("CREATE TABLE " + tablename + " (");
        fieldIt = table.iterator();
        int count = 0;
        while(fieldIt.hasNext()){
            field = (JSONObject) fieldIt.next();
            //System.out.println(field);
            datatype = (String) field.get("data_type");
            if(count == 0){
                if(!datatype.equalsIgnoreCase("varchar"))
                    buff.append(field.get("field_name")+" "+datatype);
                else{
                    buff.append(field.get("field_name")+" "+datatype+"("+field.get("char_length")+")");
                }
            }else {
                if (!datatype.equalsIgnoreCase("varchar"))
                    buff.append("," + field.get("field_name") + " " + datatype);
                else {
                    buff.append("," + field.get("field_name") + " " + datatype + "(" + field.get("char_length") + ")");
                }
            }
            count++;
        }
        buff.append(")");
        return buff.toString();
    }

    private static HashMap<String,List> getDataDictionary(Connection con) throws Exception {
        HashMap<String, List> datadictionary = new HashMap<String, List>();
        Statement stmt = null;
        ResultSet rs = null;
        JSONObject datafield = null;
        ArrayList table = null;
        String tablename = null;

        try{
            stmt = con.createStatement();
            rs = stmt.executeQuery("select * from _sys_data_dictionary");
            while (rs.next()) {
                datafield = new JSONObject();
                tablename = (String) rs.getString("table_name");
                datafield.put("table_name", tablename);
                datafield.put("field_name", rs.getString("field_name"));
                datafield.put("description", rs.getString("description"));
                datafield.put("data_type", rs.getString("data_type"));
                datafield.put("char_length", rs.getString("char_length"));
                datafield.put("acceptable_value", rs.getString("acceptable_value"));
                datafield.put("required", rs.getString("field_required"));
                datafield.put("accept_null_value", rs.getString("accept_null_value"));

                table = (ArrayList) datadictionary.get(tablename);
                if (table == null) {
                    table = new ArrayList();
                    datadictionary.put(tablename, table);
                }
                table.add(datafield);
            }
        }finally {
            if(rs != null) rs.close();
            if(stmt != null) stmt.close();
        }
        return datadictionary;
    }


    private static String getStageTableCreateSQL(String tablename, String path) throws Exception{
        StringBuffer buff = new StringBuffer();
        String token = null;
        BufferedReader br = new BufferedReader(new FileReader(path));
        String header = br.readLine();
        //System.out.println(header);
        StringTokenizer strTok = new StringTokenizer(header,"|");
        buff.append("CREATE TABLE "+tablename+" (");
        //System.out.println(token);
        int count = 0;
        while(strTok.hasMoreTokens()){
           token = strTok.nextToken();
            token = token.replaceAll("[^a-zA-Z0-9\\s+]", "").replaceAll(" ","_").toLowerCase();
            if(count == 0){
                buff.append(token+" VARCHAR("+VARCHAR_LENGTH+")");
            }else {
                if (token.equals("group"))
                    token = token + "1";
                buff.append("," + token + " VARCHAR(" + VARCHAR_LENGTH + ")");
            }
            count++;
        }
        buff.append(")");
        return buff.toString();
    }
}
