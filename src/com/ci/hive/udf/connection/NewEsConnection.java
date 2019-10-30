package com.ci.hive.udf.connection;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class NewEsConnection {
	
	private static RestHighLevelClient client=null;
	
	public static RestHighLevelClient getConnection() {
		if(client==null) {
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY,
		        new UsernamePasswordCredentials("elastic", "elastic#123"));

		RestClientBuilder builder = RestClient.builder(new HttpHost("192.168.1.133", 9200))
		        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
		            @Override
		            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
		                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
		            }
		        });
		client = new RestHighLevelClient(builder);
		}
		return client;
	}

}
