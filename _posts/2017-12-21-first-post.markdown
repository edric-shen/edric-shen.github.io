---
layout: post
title:  "MapReduce实现Text转ORC"
date:   2017-12-21 22:18:42 +0800
categories: Hadoop
tags: MapReduce
---
因工作需要，要把文本的数据转成ORC格式，用mapreduce来实现的，方式有两种，一种是用Apache Orc官方的jar包来做，另外一种是使用hive提供的的hive-exec包来做。下面贴一下两种方式的代码。
>使用Hive提供的包来实现

```
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.Arrays;


public class TextToOrc {
    public static class MapORC extends Mapper<LongWritable, Text, NullWritable, Writable> {
        private final OrcSerde orcSerde = new OrcSerde();
        private TypeInfo typeInfo = null;
        private ObjectInspector ins = null;
        private String[] fieldTypes;

        @Override
        public void setup(Context context) {
            typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(String.format("struct<%s>", context.getConfiguration().get("csv.file.schema")));
            ins = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
            String[] nameTypeArray = context.getConfiguration().get("csv.file.schema").split(",");
            fieldTypes = new String[nameTypeArray.length];

            int index = 0;
            for (String nameType : nameTypeArray) {
                fieldTypes[index] = nameType.split(":")[1];
                index++;
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\033");
            Object[] objFields = new Object[fields.length];
            for (int i = 0; i< fields.length; i++) {
                if (fields[i] == null || fields[i].length() == 0) {
                    objFields[i] = null;
                } else {
                    objFields[i] = convertType(fieldTypes[i], fields[i]);
                }
            }
            context.write(NullWritable.get(), orcSerde.serialize(Arrays.asList(objFields), ins));
        }

        private Object convertType(String type, String value) {
            Object obj = null;
            switch (type) {
                case "string":
                    obj = new String(value);
                    break;
                case "bigint":
                    obj = Long.parseLong(value);
                    break;
                case "int":
                    obj = Integer.parseInt(value);
                    break;
                default:
                    obj = new String(value);
            }

            return obj;
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("csv.file.schema", args[2]);
        Job job = Job.getInstance(conf, "TextToOrc");

        job.setJarByClass(TextToOrc.class);

        job.setMapperClass(MapORC.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(OrcNewOutputFormat.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Writable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        OrcNewOutputFormat.setOutputPath(job, new Path(args[1]));

        OrcNewOutputFormat.setCompressOutput(job, true);
        OrcNewOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

```

>使用Apache Orc官方的包来实现

```
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;
import parquet.hadoop.codec.SnappyCodec;

import java.io.IOException;

public class TextToOrc {
    public static class KeyMapper extends Mapper<LongWritable, Text, NullWritable, OrcStruct > {
        private TypeDescription schema;
        private OrcStruct pair;
        private final NullWritable nada = NullWritable.get();
        private String[] fieldTypes;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            schema = TypeDescription.fromString(String.format("struct<%s>", context.getConfiguration().get("csv.file.schema")));
            pair = (OrcStruct) OrcStruct.createValue(schema);
            String[] nameTypeArray = context.getConfiguration().get("csv.file.schema").split(",");
            fieldTypes = new String[nameTypeArray.length];

            int index = 0;
            for (String nameType : nameTypeArray) {
                fieldTypes[index] = nameType.split(":")[1];
                index++;
            }
        }


        public void map(LongWritable index, Text record, Context context)
                throws IOException, InterruptedException {
            String[] fields = record.toString().split("\033");

            int fieldIndex = 0;
            for (String field : fields) {
                pair.setFieldValue(fieldIndex, createWritable(fieldTypes[fieldIndex], field == null || field.length() == 0? "NULL" : field));
                fieldIndex++;
            }
            System.out.println(pair.toString());
            context.write(nada, pair);
        }

        private WritableComparable createWritable(String type, String value) {
            WritableComparable writable = null;
            switch (type) {
                case "string":
                    writable = new Text(value);
                    break;
                case "bigint":
                    writable = new LongWritable(Long.parseLong(value));
                    break;
                case "int":
                    writable = new IntWritable(Integer.parseInt(value));
                    break;
                default:
                    writable = new Text(value);
            }

            return writable;
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("参数个数不对");
            System.err.println("Usage: Zipper <inputPath> <outputPath> <schema>");
            System.exit(-1);
        }


        Configuration conf = new Configuration();
        conf.set("csv.file.schema", args[2]);
        conf.set("orc.mapred.output.schema", String.format("struct<%s>", args[2]));
        conf.set("mapreduce.job.outputformat.class", "org.apache.orc.mapreduce.OrcOutputFormat");
        conf.set("mapreduce.output.fileoutputformat.outputdir", args[1]);

        Job job = Job.getInstance(conf, "TextToOrc");
        job.setJarByClass(TextToOrc.class);
        job.setMapperClass(KeyMapper.class);
        job.setNumReduceTasks(0);


        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(OrcStruct.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(OrcOutputFormat.class);

        FileInputFormat.addInputPaths(job, args[0]);
        OrcOutputFormat.setCompressOutput(job, true);
        OrcOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        OrcOutputFormat.setOutputPath(job, new Path(args[1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

```
代码写的有点Low，但是是可以用的。在做的过程中，比较麻烦的是第三方jar包的处理，看网上找了好多方法，都不管用，比如-libjars参数，还有DistributedCache.addFileToClassPath, job.addArchiveToClassPath等，都不能工作，运行时报相关的类找不到。由于时间紧迫，所以也没细看。看了下mapreduce提交的代码，发现可以在jar里边建个lib目录，把用到的jar包放里边，然后一起打jar包就可以了。