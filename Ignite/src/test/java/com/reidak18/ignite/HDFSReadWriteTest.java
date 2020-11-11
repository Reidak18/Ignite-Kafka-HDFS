package com.reidak18.ignite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.*;

public class HDFSReadWriteTest
{
    HDFSWriter writer;
    FileSystem fileSystem;

    @Before
    public void setUp() throws URISyntaxException, IOException
    {
        writer = new HDFSWriter();
    }

    @Test
    public void createHdfsDirectoryTest() throws IOException, URISyntaxException {
        writer.CreateOutputDirectory("test");

        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);
        Path hdfsWritePath = new Path("test");
        assertTrue(fileSystem.exists(hdfsWritePath));
    }

    @Test
    public void writeFileToHdfsTest() throws IOException, URISyntaxException
    {
        writer.writeFileToHDFS("test", "/user/root/test/test.txt");

        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);
        Path hdfsWritePath = new Path("/user/root/test/test.txt");
        assertTrue(fileSystem.exists(hdfsWritePath));
    }

    @Test
    public void readFileToHdfsTest() throws IOException, URISyntaxException
    {
        String test = writer.ReadFileFromHDFS("/user/root/test/test.txt");

        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);
        Path hdfsWritePath = new Path("/user/root/test/test.txt");
        assertEquals("test", test);
    }
}
