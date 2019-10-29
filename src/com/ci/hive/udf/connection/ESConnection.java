package com.ci.hive.udf.connection;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;


public class ESConnection {
	// elastic cluster name
		private static String clusterName = "es_intelligence";

		// ElasticSearch http port defaul to 9300
		private static int port = 9300;

		// Elasticsearch client node ip
		private static String hostName = "192.168.1.132";
		// close Client
		private static Client client = null;
		

		// To establish a new Connection
		public static Client getConnection() {
			/*client = new PreBuiltXPackTransportClient(Settings.builder()
			        .put("cluster.name", "myClusterName")
			        .put("xpack.security.user", "transport_client_user:x-pack-test-password")
			        .build())
			    .addTransportAddress(new TransportAddress("localhost", 9300))
			    .addTransportAddress(new TransportAddress("localhost", 9301));*/
			if (client == null) {
				Settings settings = Settings.builder().put("cluster.name", clusterName).put("client.transport.sniff", true)
						.put("xpack.security.user", "elastic:elastic#123")
						.build();
				try {
					 client = new PreBuiltXPackTransportClient(settings)
							.addTransportAddress(new TransportAddress(InetAddress.getByName(hostName), 9300));
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}

			}
			return client;
		}

		public static String getClusterName() {
			return clusterName;
		}

		public static void setClusterName(String clusterName) {
			ESConnection.clusterName = clusterName;
		}

		public static int getPort() {
			return port;
		}

		public static void setPort(int port) {
			ESConnection.port = port;
		}

		public static String getHostName() {
			return hostName;
		}

		public static void setHostName(String hostName) {
			ESConnection.hostName = hostName;
		}

		public static void closeConnection() {
			client.close();
		}
		
		
		
}
