package com.ci.hive.udf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;

public class CallApi extends UDF {
	
	public String evaluate(String incoming, String state_key, String other, String celltowerid, String calling_no, String startonlytime, String starttime, String roaming_nw, String duration, String called_no, String imeinumber, String last_cellid, String imsinumber, String otherinfo, String phone, String asondate, String provider_key, String first_cellid, String call_type, String tower_key, String ucid) {
		//String[] input= {"incoming","state_key","other","celltowerid","calling_no","startonlytime","starttime","roaming_nw","duration","called_no","imeinumber","last_cellid","imsinumber","otherinfo","phone","asondate","provider_key","first_cellid","call_type","tower_key","ucid"};
		Map input=new HashMap();
		input.put("incoming",incoming);
		input.put("state_key",state_key);
		input.put("other",other);
		input.put("celltowerid",celltowerid);
		input.put("calling_no",calling_no);
		input.put("startonlytime",startonlytime);
		input.put("starttime",starttime);
		input.put("roaming_nw",roaming_nw);
		input.put("duration",duration);
		input.put("called_no",called_no);
		input.put("imeinumber",imeinumber);
		input.put("last_cellid",last_cellid);
		input.put("imsinumber",imsinumber);
		input.put("otherinfo",otherinfo);
		input.put("phone",phone);
		input.put("asondate",asondate);
		input.put("provider_key",provider_key);
		input.put("first_cellid",first_cellid);
		input.put("call_type",call_type);
		input.put("tower_key",tower_key);
		input.put("ucid",ucid);
		
		try {

			URL url = new URL("http://localhost:8080/RESTfulExample/json/product/get");
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");

			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ conn.getResponseCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(
				(conn.getInputStream())));

			String output;
			System.out.println("Output from Server .... \n");
			while ((output = br.readLine()) != null) {
				System.out.println(output);
			}

			conn.disconnect();

		  } catch (MalformedURLException e) {

			e.printStackTrace();

		  } catch (IOException e) {

			e.printStackTrace();

		  }

		return input.toString();
	}

}
