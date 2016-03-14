package com.example.cassandra.test;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class CassandraShowTableTest {

	public static void main(String args[]) throws Exception{
		String KEYSPACE = "system_schema";
		
		TTransport tr = new TSocket("192.168.1.225", 9160);
		TFramedTransport tf = new TFramedTransport(tr);
		TProtocol proto = new TBinaryProtocol(tf);
		
		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();
		client.set_keyspace(KEYSPACE);
		
		ByteBuffer query = ByteBuffer.wrap("select table_name from tables where keyspace_name='demo'".getBytes());
		CqlResult result = client.execute_cql3_query(query, Compression.NONE, ConsistencyLevel.ALL);
		List<CqlRow> cqlRows = result.getRows();
		
		for(CqlRow cqlRow : cqlRows){
			List<Column> columns = cqlRow.getColumns();
			String tableName = ByteBufferUtil.string(ByteBuffer.wrap(columns.get(0).getValue()));
			System.out.println(tableName);
		}
		tr.close();
	}
}
