package com.ci.hive.udf;


import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.ci.hive.udf.connection.ESConnection;

public class CDR_Tower extends UDF {
	
	public Text evaluate(Text s) {
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
		return new Text ("success");
		}else {
			return new Text ("fail null");
		}
		
	}
	
	
	//towerkey direct join
	//celltowerid >5 direct join
	//celltowerid <=5 rely on statekey & provider key
	
	public void getCellTowerDetails(Map inputMap,String[] input,String[] rec) {
		SearchRequestBuilder searchRequestBuilder=ESConnection.getConnection().prepareSearch("ci-celltower");
		String towerKey=inputMap.get("tower_key").toString();
		String celltowerid=inputMap.get("celltowerid").toString();
		String statekey=inputMap.get("state_key").toString();
		String providerKey=inputMap.get("provider_key").toString();
		String starttime=inputMap.get("starttime").toString();
		if(towerKey!=null && StringUtils.isNotBlank(towerKey)) {
			searchRequestBuilder.setQuery(QueryBuilders.termQuery("tower_key", towerKey));
		}else if(celltowerid.length()>5) {
			BoolQueryBuilder boolQuery=QueryBuilders.boolQuery();
			boolQuery.must(QueryBuilders.termQuery("celltowerid", celltowerid));
			boolQuery.must(QueryBuilders.rangeQuery("lastupdate").lte(starttime));
			searchRequestBuilder.setQuery(boolQuery);
		}else if(celltowerid.length()<=5 && statekey!=null && StringUtils.isNotBlank(statekey) && providerKey!=null && StringUtils.isNotBlank(providerKey)) {
			BoolQueryBuilder boolQuery=QueryBuilders.boolQuery();
			boolQuery.must(QueryBuilders.termQuery("celltowerid", celltowerid));
			boolQuery.must(QueryBuilders.rangeQuery("lastupdate").lte(starttime));
			boolQuery.must(QueryBuilders.termQuery("state_key", statekey));
			boolQuery.must(QueryBuilders.termQuery("provider_key", providerKey));
			searchRequestBuilder.setQuery(boolQuery);
		}
		
		searchRequestBuilder.addSort("lastupdate", SortOrder.DESC);
		searchRequestBuilder.setSize(1);
		SearchResponse serp=searchRequestBuilder.execute().actionGet();
		
		SearchHit[] hits=serp.getHits().getHits();
		Map resp=new HashMap();
		for(SearchHit hit:hits) {
			resp=hit.getSourceAsMap();
			break;
		}
		
		int i=0;
		for(String field:input) {
			resp.put("cdr-"+field, rec[i]);
			i++;
		}
		indexDoc(resp);
		
	}
	
	public void indexDoc(Map doc) {
		IndexRequestBuilder indexReq=ESConnection.getConnection().prepareIndex("ci-cdrsumm", "docs");
		indexReq.setSource(doc);
		indexReq.execute().actionGet();
	}
	
}
