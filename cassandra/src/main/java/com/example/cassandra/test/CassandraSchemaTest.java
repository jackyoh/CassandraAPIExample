package com.example.cassandra.test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlMetadata;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class CassandraSchemaTest {

	public static void main(String args[]) throws Exception{
		String KEYSPACE = "demo";
		
		TTransport tr = new TSocket("192.168.1.225", 9160);
		TFramedTransport tf = new TFramedTransport(tr);
		TProtocol proto = new TBinaryProtocol(tf);
		
		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();
		client.set_keyspace(KEYSPACE);
		
		ByteBuffer query = ByteBuffer.wrap("select * from users".getBytes());
		CqlResult result = client.execute_cql3_query(query, Compression.NONE, ConsistencyLevel.ALL);
		
		CqlMetadata metadata = result.getSchema();
		Map<ByteBuffer, String> maps = metadata.getName_types();
		
		//Cassandra Code BytesSerializer.java   IntegerTypeTest.java
		for(Entry<ByteBuffer, String> e : maps.entrySet()){
			System.out.println(ByteBufferUtil.string(e.getKey()) + "    " + e.getValue());
		}
		tr.close();
	}
}
