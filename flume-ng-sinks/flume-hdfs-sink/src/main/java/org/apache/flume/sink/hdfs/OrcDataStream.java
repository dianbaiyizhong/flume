//package org.apache.flume.sink.hdfs;
//
//import com.google.common.annotations.VisibleForTesting;
//import com.google.common.collect.Lists;
//import org.apache.commons.lang3.RandomUtils;
//import org.apache.flume.Event;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
//import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
//import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
//import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
//import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
//import org.apache.hadoop.hive.serde2.SerDeException;
//import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
//import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.SequenceFile;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.io.compress.CompressionCodec;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.OutputFormat;
//import org.apache.hadoop.mapred.RecordWriter;
//import org.apache.hadoop.mapred.Reporter;
//import org.apache.orc.TypeDescription;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//public class OrcDataStream extends AbstractHDFSWriter {
//    private static final Logger logger = LoggerFactory.getLogger(OrcDataStream.class);
//
//
//    private RecordWriter writer;
//
//    private OrcSerde serde;
//
//    private StructObjectInspector inspector =
//            (StructObjectInspector) ObjectInspectorFactory
//                    .getReflectionObjectInspector(MyRow.class,
//                            ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
//
//
//    @Override
//    public void open(String filePath) throws IOException {
//        logger.info("OrcDataStream open...");
//        Configuration conf = new Configuration();
//        JobConf jobConf = new JobConf();
//        FileSystem fs = getDfs(conf, new Path(filePath));
//        serde = new OrcSerde();
//        OutputFormat outFormat = new OrcOutputFormat();
//        writer = outFormat.getRecordWriter(fs, jobConf,
//                filePath, Reporter.NULL);
//    }
//
//    @VisibleForTesting
//    protected FileSystem getDfs(Configuration conf, Path dstPath) throws IOException {
//        return dstPath.getFileSystem(conf);
//    }
//
//
//    @Override
//    public void open(String filePath, CompressionCodec codec, SequenceFile.CompressionType cType) throws IOException {
//        open(filePath);
//
//    }
//
//
//    @Override
//    public void append(Event event) throws IOException, SerDeException {
//
//        TypeDescription schema = null;
//
//        VectorizedRowBatch batch = schema.createRowBatch();
//
////        serde.serialize(batch,inspector);
////        writer.write(NullWritable.get(),
////                serde.serialize(new MyRow("张三", RandomUtils.nextInt(10, 500)), inspector));
////        writer.write(NullWritable.get(),
////                serde.serialize(new MyRow("张三", 20), inspector));
//
//        String body = new String(event.getBody());
//
//        List<String> fieldList = Lists.newArrayList("name", "sex");
//
//        List<? super ColumnVector> columnVectors = new ArrayList<>();
//
//        serde.serialize()
//        for (int i = 0; i < fieldList.size(); i++) {
//            try {
//                writer.write(NullWritable.get(),
//                        serde.serializeVector(batch, inspector));
//            } catch (Exception e) {
//
//
//            }
//
//        }
//
//    }
//
//    @Override
//    public void sync() throws IOException {
//        logger.info("OrcDataStream sync.............");
//    }
//
//    @Override
//    public void close() throws IOException {
//        logger.info("OrcDataStream close.............");
//        writer.close(Reporter.NULL);
//    }
//
//    static class MyRow implements Writable {
//        String name;
//        int age;
//
//        MyRow(String name, int age) {
//            this.name = name;
//            this.age = age;
//        }
//
//        @Override
//        public void readFields(DataInput arg0) throws IOException {
//            throw new UnsupportedOperationException("no write");
//        }
//
//        @Override
//        public void write(DataOutput arg0) throws IOException {
//            throw new UnsupportedOperationException("no read");
//        }
//
//    }
//
//}
