package org.apache.flume.sink.hdfs;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Event;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class OrcDataStream2 extends AbstractHDFSWriter {
    private static final Logger logger = LoggerFactory.getLogger(OrcDataStream2.class);


    private RecordWriter writer;

    private OrcSerde serde;




    @Override
    public void open(String filePath) throws IOException {
        logger.info("OrcDataStream open...");
        Configuration conf = new Configuration();
        JobConf jobConf = new JobConf();
        FileSystem fs = getDfs(conf, new Path(filePath));
        serde = new OrcSerde();
        OutputFormat outFormat = new OrcOutputFormat();
        writer = outFormat.getRecordWriter(fs, jobConf,
                filePath, Reporter.NULL);
    }

    @VisibleForTesting
    protected FileSystem getDfs(Configuration conf, Path dstPath) throws IOException {
        return dstPath.getFileSystem(conf);
    }


    @Override
    public void open(String filePath, CompressionCodec codec, SequenceFile.CompressionType cType) throws IOException {
        open(filePath);

    }

    public static final String WRITE_KEY = "WRITE_KEY";

    @Override
    public void append(Event event) throws IOException {




        String key = event.getHeaders().get(WRITE_KEY);
        byte[] byteBody = event.getBody();
        String body = new String(byteBody, Charsets.UTF_8);

        List<List<String>> rowList = new Gson().fromJson(body, List.class);

        List<String> fieldList = OrcSchemaManager.getInstance().getSchema(key).getFieldNames().subList(0,8);
        List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(
                StringUtils.repeat("string", ":", 9)
        );

        TypeInfo rowTypeInfo = TypeInfoFactory.getStructTypeInfo(fieldList, columnTypes);
        StructObjectInspector inspector = new ArrayWritableObjectInspector((StructTypeInfo) rowTypeInfo);

         /*StructObjectInspector inspector2 =
                (StructObjectInspector) ObjectInspectorFactory
                        .getReflectionObjectInspector(MyRow.class,
                                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);*/
        for (List<String> line : rowList) {
//            writer.write(NullWritable.get(),
//                    serde.serialize(new MyRow("张三", RandomUtils.nextInt(10, 500)), inspector));

//            writer.write(NullWritable.get(),
//                    serde.serialize(new MyRow("张三", 20), inspector));
               writer.write(NullWritable.get(), serde.serialize(line, inspector));

        }

    }

    @Override
    public void sync() throws IOException {
        logger.info("OrcDataStream sync.............");
    }

    @Override
    public void close() throws IOException {
        logger.info("OrcDataStream close.............");
        writer.close(Reporter.NULL);
    }

    static class MyRow implements Writable {
        String name;
        int age;

        MyRow(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public void readFields(DataInput arg0) throws IOException {
            throw new UnsupportedOperationException("no write");
        }

        @Override
        public void write(DataOutput arg0) throws IOException {
            throw new UnsupportedOperationException("no read");
        }

    }
}
