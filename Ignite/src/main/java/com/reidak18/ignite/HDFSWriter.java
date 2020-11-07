package com.reidak18.ignite;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.io.IOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSWriter
{
//    public void WriteToFile(String path) throws IOException
//    {
//        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
//    }
        public void SaveInputToHDFS(String inputs) throws IOException, URISyntaxException
        {
            CreateOutputDirectory("input");
            writeFileToHDFS(inputs, "/user/root/input/input.txt");
        }

        public void SaveResultsToHDFS(String results) throws IOException, URISyntaxException
        {
            CreateOutputDirectory("output");
            writeFileToHDFS(results, "/user/root/output/output.txt");
        }

        private void CreateOutputDirectory(String directoryName) throws IOException
        {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "hdfs://localhost:9000");
            FileSystem fileSystem = FileSystem.get(configuration);
            Path path = new Path(directoryName);
            fileSystem.mkdirs(path);
        }

        private void writeFileToHDFS(String str, String path) throws IOException, URISyntaxException
        {
            Configuration configuration = new Configuration();
            configuration.set("dfs.client.use.datanode.hostname", "true");
            configuration.set("fs.defaultFS", "hdfs://localhost:9000");
//            FileSystem fileSystem = FileSystem.get(configuration);
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);
            Path hdfsWritePath = new Path(path);

            if (fileSystem.exists(hdfsWritePath))
            {
                fileSystem.delete(hdfsWritePath, false);
            }

            FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath, true);
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
            bufferedWriter.write(str);
            bufferedWriter.close();
            fileSystem.close();
        }

    public String ReadFileFromHDFS(String path) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        //Create a path
        Path hdfsReadPath = new Path(path);
        //Init input stream
        FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
        //Classical input stream usage
        String out = IOUtils.toString(inputStream, "UTF-8");
        /*BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        String line = null;
        while ((line=bufferedReader.readLine())!=null){
            System.out.println(line);
        }*/
        inputStream.close();
        fileSystem.close();
        return out;
    }
}