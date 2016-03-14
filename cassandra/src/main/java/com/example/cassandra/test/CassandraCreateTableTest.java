package com.example.cassandra.test;

import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class CassandraCreateTableTest {
	public static void main(String args[]) throws Exception{
		String KEYSPACE = "demo";
		
		TTransport tr = new TSocket("192.168.1.225", 9160);
		TFramedTransport tf = new TFramedTransport(tr);
		TProtocol proto = new TBinaryProtocol(tf);
		
		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();
		client.set_keyspace(KEYSPACE);
		
		ByteBuffer query = ByteBuffer.wrap("create table table2(column1 text, column2 text, PRIMARY KEY(column1));".getBytes());
		
		client.execute_cql3_query(query, Compression.NONE, ConsistencyLevel.ALL);
		tr.close();
	}
}
