package com.example.cassandra.test;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class CassnadraClusterTest {
	
	public static void main(String args[]) throws Exception{
		
		String KEYSPACE = "demo";
		
		TTransport tr = new TSocket("192.168.1.225", 9160);
		TFramedTransport tf = new TFramedTransport(tr);
		TProtocol proto = new TBinaryProtocol(tf);
		
		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();
		client.set_keyspace(KEYSPACE);

		String clusterName = client.describe_cluster_name();
		System.out.println(clusterName);
		
		Map<String, List<String>> versionMap = client.describe_schema_versions();
		
		for(Entry<String, List<String>> e : versionMap.entrySet()){
			System.out.println(e.getKey());
			for(String value : e.getValue()){
				System.out.println(value);
			}
		}
		tr.close();
	}

}
