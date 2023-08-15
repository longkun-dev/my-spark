package com.chenxii.myspark.sparksql.ch9;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 屏蔽客户姓名的 UDF
 * 1、package spark-sql-1.1.0.jar
 * 2、hive > add jar spark-sql-1.1.0.jar
 * 3、create temporary function shield_client_name as "com.chenxii.myspark.sparksql.ch9.ShieldClientName"
 */
public class ShieldClientName extends UDF {

    public Text evaluate(Text clientName) {
        if (clientName == null) {
            return null;
        }

        String clientNameString = clientName.toString();
        if (clientNameString == null || "".equals(clientNameString) || clientNameString.length() < 2) {
            return clientName;
        }

        return new Text(clientNameString.charAt(0) + "**");
    }

}
