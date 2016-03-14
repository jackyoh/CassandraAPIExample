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

public class CassandraInsertTest {
	
	public static void main(String args[]) throws Exception{
		String KEYSPACE = "demo";
		
		TTransport tr = new TSocket("192.168.1.225", 9160);
		TFramedTransport tf = new TFramedTransport(tr);
		TProtocol proto = new TBinaryProtocol(tf);
		
		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();
		client.set_keyspace(KEYSPACE);
		
		ByteBuffer sql = ByteBuffer.wrap("INSERT INTO table1(column1, column2) VALUES('a1', 'a2')".getBytes());
		
		client.execute_cql3_query(sql, Compression.NONE, ConsistencyLevel.ALL);
		
		sql = ByteBuffer.wrap("INSERT INTO table1(column1, column2) VALUES('b1', 'b2')".getBytes());
		client.execute_cql3_query(sql, Compression.NONE, ConsistencyLevel.ALL);
		
		tr.close();
	}

}
