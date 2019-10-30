package com.ci.hive.udf;


import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.ci.hive.udf.connection.ESConnection;

public class CDR_Tower extends UDF  {
	
	/*public String evaluate(String s) {
		System.out.println(s+" enterenssssssssssssssssssss");
		if(s.toString()!=null && StringUtils.isNotBlank(s.toString())) {
		String[] rec = s.toString().split("$");
		String[] input= {"incoming","state_key","other","celltowerid","calling_no","startonlytime","starttime","roaming_nw","duration","called_no","imeinumber","last_cellid","imsinumber","otherinfo","phone","asondate","provider_key","first_cellid","call_type","tower_key","ucid"};
		Map inputMap=new HashMap();
		int i=0;
		for(String field:input) {
			inputMap.put(field, rec[i]);
			i++;
		}
	//	getCellTowerDetails(inputMap,input,rec);
		return new String ("success");
		}else {
			return new String ("fail null");
		}
		
	}*/
	
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
			
			ESServiceUtils esUtils=new ESServiceUtils();
			String out=esUtils.getCellTowerDetails(input);
			//esUtils.indexDoc(input);
			
			return out;
	  }
	 
	 
	
	/*public static void main(String[] args) {
		CDR_Tower cdr_Tower = new CDR_Tower();
		cdr_Tower.evaluate("1","1","381","40407-62-9092","null","15:26:13.0","2012-03-22 07:54:37.0","ROAMING_MAHARASHTRA","58","null","0","null","null","MUMBAI","7738593662","2012-10-15 17:41:26.657","6","null","null","22","184350829");
		System.out.println("sdsdsdsds");
	}*/
	 
	//towerkey direct join
	//celltowerid >5 direct join
	//celltowerid <=5 rely on statekey & provider key
	
	
	
}
