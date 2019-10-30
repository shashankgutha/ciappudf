package com.ci.hive.udf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.ci.hive.udf.connection.ESConnection;
import com.ci.hive.udf.connection.NewEsConnection;

public class ESServiceUtils {
	
	public String getCellTowerDetails(Map inputMap) {
		String output="";
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//SearchRequestBuilder searchRequestBuilder=ESConnection.getConnection().prepareSearch("ci-celltower");
		String towerKey=inputMap.get("tower_key").toString();
		String celltowerid=inputMap.get("celltowerid").toString();
		String statekey=inputMap.get("state_key").toString();
		String providerKey=inputMap.get("provider_key").toString();
		String starttime=inputMap.get("starttime").toString();
		BoolQueryBuilder boolQuery=QueryBuilders.boolQuery();
		if(towerKey!=null && StringUtils.isNotBlank(towerKey)) {
			boolQuery.must(QueryBuilders.termQuery("tower_key", towerKey));
		}else if(celltowerid!=null && celltowerid.length()>5) {
			boolQuery.must(QueryBuilders.termQuery("celltowerid.keyword", celltowerid));
			boolQuery.must(QueryBuilders.rangeQuery("lastupdate").lte(starttime));
			
		}else if(celltowerid!=null && celltowerid.length()<=5 && statekey!=null && StringUtils.isNotBlank(statekey) && providerKey!=null && StringUtils.isNotBlank(providerKey)) {
			boolQuery.must(QueryBuilders.termQuery("celltowerid.keyword", celltowerid));
			boolQuery.must(QueryBuilders.rangeQuery("lastupdate").lte(starttime.replaceAll("\\s+", " ")));
			boolQuery.must(QueryBuilders.termQuery("state_key", statekey));
			boolQuery.must(QueryBuilders.termQuery("provider_key", providerKey));
		}
		
		if(boolQuery.hasClauses()) {
			searchSourceBuilder.query(boolQuery);
			searchSourceBuilder.sort("lastupdate", SortOrder.DESC);
			searchSourceBuilder.size(1);
			System.out.println(searchSourceBuilder);
			SearchResponse serp=getSearch("ci-celltower",searchSourceBuilder);
			System.out.println(serp.getHits().totalHits+" hjhjhjhjh");
		//SearchResponse serp=searchRequestBuilder.execute().actionGet();
		
		SearchHit[] hits=serp.getHits().getHits();
		Map towerresp=new HashMap();
		System.out.println(hits.length+" hjhjhjhjh");
		for(SearchHit hit:hits) {
			towerresp=hit.getSourceAsMap();
		}
		
		int i=0;
		
		Map pushMap=new HashMap();
		
		for(Object field:inputMap.keySet().toArray()) {
			if(i==0) {
				if(inputMap.get(field)!=null) {
					output=inputMap.get(field).toString();
					pushMap.put("cdr-"+field, inputMap.get(field).toString());
				}else {
					output=" ";
					pushMap.put("cdr-"+field, "");
				}
				
			}else{
				if(inputMap.get(field)!=null) {
					pushMap.put("cdr-"+field, inputMap.get(field).toString());
				output=output+"|"+inputMap.get(field);
				}else {
					pushMap.put("cdr-"+field, "");
					output=output+"|"+" ";
				}
			}
			
			
			//resp.put("cdr-"+field, inputMap.get(field));
			i++;
		}
		
		
		String[] towerK= {"tower_key","celltowerid","bts_id","areadescription","siteaddress","lat","long","azimuth","operator","state","otype","lastupdate","opid","state_key","provider_name","provider_key","state_code","cellid","state_name"};
		
		for(String towerF:towerK) {
			if(towerresp.get(towerF)!=null) {
				pushMap.put(towerF, towerresp.get(towerF));
				output=output+"|"+towerresp.get(towerF).toString();
			}else {
				pushMap.put(towerF, "");
				output=output+"|"+" ";
			}
		}
		
		System.out.println(pushMap);
		indexDoc(pushMap);
		
		}
		
		
		System.out.println(output);
		return output;
		
	}
	
	public void indexDoc(Map doc) {
		//IndexRequestBuilder indexReq=ESConnection.getConnection().prepareIndex("ci-cdrsumm", "docs");
		RestHighLevelClient client=NewEsConnection.getConnection();
		IndexRequest indexReq= new IndexRequest("ci-cdrsumm", "docs");
		indexReq.source(doc);
		try {
			client.index(indexReq,RequestOptions.DEFAULT);
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		//indexReq.execute().actionGet();
	}
	
	
	public SearchResponse getSearch(String indexName, SearchSourceBuilder sourceBuilder) {
		SearchRequest searchRequest = new SearchRequest(indexName);
		searchRequest.source(sourceBuilder);
		
		try {
			return NewEsConnection.getConnection().search(searchRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("exception");
			return null;
		}
	}

}
