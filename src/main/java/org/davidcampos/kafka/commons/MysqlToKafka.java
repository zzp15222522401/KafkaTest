package org.davidcampos.kafka.commons;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.davidcampos.kafka.producer.KafkaProducerExample;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class MysqlToKafka {
    public static void main(String[] args) throws Exception{
        MysqlToKafka a = new MysqlToKafka();
        Connection con = a.getCon();
        String sql = "select * from userconfig";
        Statement statement = con.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        JSONArray objects = resultSetToJson(rs);
        for (int i=0;i<objects.size();i++){
            System.out.println(objects.get(i));
            KafkaProducerExample.sendMessage(Commons.EXAMPLE_KAFKA_TOPIC, String.valueOf(objects.get(i)));
        }

    }

    public  Connection getCon() throws IOException {
        String propFileName = "test_mysql_conn.properties";
        Properties properties = loadProperties(propFileName);
        String username = properties.getProperty("jdbc.username");
        String password = properties.getProperty("jdbc.password");
        String dbName = properties.getProperty("jdbc.dbName");
        String host = properties.getProperty("jdbc.host");
        String port = properties.getProperty("jdbc.port");
        String driver = properties.getProperty("jdbc.driverClass");
        String url = "jdbc:mysql://" + host + ":" + port + "/" + dbName + "?characterEncoding=utf-8&allowMultiQueries=true&useSSL=false";
        Connection conn=null;
        try{
            Class.forName(driver);
            conn = DriverManager.getConnection(url,username,password);
        }catch(Exception e){
            e.printStackTrace();
        }
        return conn;
    }

    public static Properties loadProperties(String fileName) throws IOException {

        Properties properties = new Properties();
        InputStream propFile = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        properties.load(propFile);
        return properties;
    }

    /**
     * ResultSet???JSON
     */
    public static JSONArray resultSetToJson(ResultSet rs) throws SQLException, JSONException, UnsupportedEncodingException {
        // json??????
        JSONArray array = new JSONArray();
        // ????????????
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        // ??????ResultSet??????????????????
        while (rs.next()) {
            JSONObject jsonObj = new JSONObject();
            // ???????????????
            for (int i = 1; i <= columnCount; i++) {
                String value = null;
                String columnName = metaData.getColumnLabel(i);//?????????
                if (rs.getString(columnName) != null && !rs.getString(columnName).equals("")) {
                    value = new String(rs.getBytes(columnName), "UTF-8");//?????????,??????????????????
                    //  System.out.println("===" + value);
                } else {
                    value = "";//????????????????????????????????????
                }
                jsonObj.put(columnName, value);
            }
            array.add(jsonObj);
        }
        rs.close();
        return array;
    }
}
