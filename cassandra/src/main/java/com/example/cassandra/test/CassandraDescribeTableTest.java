package com.example.cassandra.test;

import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.KsDef;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class CassandraDescribeTableTest {

	public static void main(String args[]) throws Exception{
		TTransport tr = new TSocket("192.168.1.225", 9160);
		TFramedTransport tf = new TFramedTransport(tr);
		TProtocol proto = new TBinaryProtocol(tf);
		
		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();

		
		List<KsDef> ksDefs = client.describe_keyspaces();
		
		for(KsDef ksDef : ksDefs){
			System.out.println(ksDef.getName() + "    " + ksDef.getCf_defs().size());
		}
		
		tr.close();
	}
	
}
