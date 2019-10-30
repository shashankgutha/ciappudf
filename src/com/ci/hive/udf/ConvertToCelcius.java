package com.ci.hive.udf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ConvertToCelcius extends UDF {
    public Text evaluate(Text incoming, Text state_key, Text other, Text celltowerid, Text calling_no, Text startonlytime, Text starttime, Text roaming_nw, Text duration, Text called_no, Text imeinumber, Text last_cellid, Text imsinumber, Text otherinfo, Text phone, Text asondate, Text provider_key, Text first_cellid, Text call_type, Text tower_key, Text ucid) {
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

		return new Text (input.toString()+"");
  }
}
