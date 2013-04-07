/*
 * Copyright Notice ====================================================
 * This file contains proprietary information of Hewlett-Packard Co.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2009   All rights reserved. ======================
 */

package com.leopold.cassandra.demo.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class Test
{
    public static void main(String[] args)
        throws Exception
    {
        Cassandra.Iface client = createConnection();
        client.set_keyspace("wordcount");

        SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange()
                .setStart(ByteBufferUtil.EMPTY_BYTE_BUFFER).setFinish(ByteBufferUtil.EMPTY_BYTE_BUFFER).setCount(100));

        List<TokenRange> list = client.describe_ring("wordcount");

        TokenRange tokenRange = list.get(0);
        //        for (TokenRange tokenRange : list)
        //        {
        //            client.describe_splits_ex("input_words", tokenRange.start_token, tokenRange.end_token, 10);
        //        }
        client.describe_splits_ex("input_words", tokenRange.start_token, tokenRange.end_token, 10);

        KeyRange keyRange = new KeyRange().setStart_token(tokenRange.getStart_token()).setEnd_token(
                tokenRange.getEnd_token());

        List<KeySlice> rows = client.get_range_slices(new ColumnParent("input_words"), predicate, keyRange,
                ConsistencyLevel.ONE);
        for (KeySlice keySlice : rows)
        {
            //            System.out.println(keySlice);
            Iterator<ColumnOrSuperColumn> columns = keySlice.columns.iterator();
            if (columns.hasNext())
            {
                ColumnOrSuperColumn cosc = columns.next();
                System.out.println(cosc);
            }
        }
    }

    protected List<Column> unthriftify(ColumnOrSuperColumn cosc)
    {
        if (cosc.counter_column != null)
            return Collections.<Column> singletonList(unthriftifyCounter(cosc.counter_column));
        if (cosc.counter_super_column != null)
            return unthriftifySuperCounter(cosc.counter_super_column);
        if (cosc.super_column != null)
            return unthriftifySuper(cosc.super_column);
        assert cosc.column != null;
        return Collections.<Column> singletonList(unthriftifySimple(cosc.column));
    }

    private List<Column> unthriftifySuperCounter(CounterSuperColumn super_column)
    {
        List<Column> columns = new ArrayList<Column>(super_column.columns.size());
        for (CounterColumn column : super_column.columns)
        {
            Column c = unthriftifyCounter(column);
            //            columns.add(c.withUpdatedName(CompositeType.build(super_column.name, c.name())));
        }
        return columns;
    }

    private List<Column> unthriftifySuper(SuperColumn super_column)
    {
        List<Column> columns = new ArrayList<Column>(super_column.columns.size());
        for (org.apache.cassandra.thrift.Column column : super_column.columns)
        {
            Column c = unthriftifySimple(column);
            //            columns.add(c.withUpdatedName(CompositeType.build(super_column.name, c.name())));
        }
        return columns;
    }

    protected Column unthriftifySimple(org.apache.cassandra.thrift.Column column)
    {
        return new Column(column.name, column.value, column.timestamp);
    }

    private Column unthriftifyCounter(CounterColumn column)
    {
        //CounterColumns read the counterID from the System table, so need the StorageService running and access
        //to cassandra.yaml. To avoid a Hadoop needing access to yaml return a regular Column.
        return new Column(column.name, ByteBufferUtil.bytes(column.value), 0);
    }
    private static Cassandra.Iface createConnection()
        throws TTransportException
    {
        if (System.getProperty("cassandra.host") == null || System.getProperty("cassandra.port") == null)
        {

        }
        return createConnection("16.158.83.35", Integer.valueOf("9160"));
    }

    private static Cassandra.Client createConnection(String host, Integer port)
        throws TTransportException
    {
        TSocket socket = new TSocket(host, port);
        TTransport trans = new TFramedTransport(socket);
        trans.open();
        TProtocol protocol = new TBinaryProtocol(trans);

        return new Cassandra.Client(protocol);
    }
}
