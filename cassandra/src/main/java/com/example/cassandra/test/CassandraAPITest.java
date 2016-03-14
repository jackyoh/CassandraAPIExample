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

public class CassandraAPITest {
	
	public static void main(String args[]) throws Exception{
		String KEYSPACE = "demo";
		
		TTransport tr = new TSocket("192.168.1.225", 9160);
		TFramedTransport tf = new TFramedTransport(tr);
		TProtocol proto = new TBinaryProtocol(tf);
		
		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();
		client.set_keyspace(KEYSPACE);
		
		ByteBuffer query = ByteBuffer.wrap("SELECT * FROM users WHERE lastname='Simth'".getBytes());
		CqlResult result = client.execute_cql3_query(query, Compression.NONE, ConsistencyLevel.ALL);
		
		System.out.println("Result Rows:" + result.getRows().size());//show table rows
		

		List<CqlRow> cqlRows = result.getRows();
		/*for(CqlRow cqlRow : cqlRows){
			List<Column> columns = cqlRow.getColumns();
			for(Column column : columns){
				String str = new String(column.getValue());
				System.out.println(str);
			}
		}*/
		
		for(CqlRow cqlRow : cqlRows){
			List<Column> columns = cqlRow.getColumns();
			int key = ByteBufferUtil.toInt(ByteBuffer.wrap(columns.get(1).getValue()));//show age field data
			System.out.println(key);
		}
		tr.close();
	}

}
