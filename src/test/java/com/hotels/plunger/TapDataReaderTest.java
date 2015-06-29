/**
 * Copyright 2015 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.plunger;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import cascading.tap.partition.DelimitedPartition;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class TapDataReaderTest {

  private final Fields partitionFields = new Fields("C", String.class);
  private final Fields valueFields = new Fields(Fields.names("A", "B"), Fields.types(Integer.TYPE, String.class));
  private final Fields fields = Fields.join(valueFields, partitionFields);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final Data expected = new Data(fields, Arrays.asList(new Tuple(1, "hello", "X"), new Tuple(2, "aloha", "Y")));

  @Test
  public void readLocal() throws IOException {
    File tsvFile = temporaryFolder.newFile("data.tsv");
    FileUtils.writeStringToFile(tsvFile, "1\thello\tX\n2\taloha\tY\n", Charset.forName("UTF-8"));

    cascading.tap.local.FileTap fileTap = new cascading.tap.local.FileTap(new cascading.scheme.local.TextDelimited(
        fields), tsvFile.getAbsolutePath());

    Data actual = new TapDataReader(fileTap).read();

    assertThat(actual, is(expected));
  }

  @Test
  public void readLocalPartitions() throws IOException {
    File tsvFolder = temporaryFolder.newFolder("data");
    File tsvFileX = new File(tsvFolder, "X");
    File tsvFileY = new File(tsvFolder, "Y");

    FileUtils.writeStringToFile(tsvFileX, "1\thello\n", Charset.forName("UTF-8"));
    FileUtils.writeStringToFile(tsvFileY, "2\taloha\n", Charset.forName("UTF-8"));

    cascading.tap.local.PartitionTap partitionTap = new cascading.tap.local.PartitionTap(
        new cascading.tap.local.FileTap(new cascading.scheme.local.TextDelimited(valueFields),
            tsvFolder.getAbsolutePath()), new DelimitedPartition(partitionFields));

    Data actual = new TapDataReader(partitionTap).read();

    assertThat(actual.orderBy(fields).asTupleEntryList(), is(expected.orderBy(fields).asTupleEntryList()));
  }

  @Test
  public void readLocalPartitionsWithHiddenFile() throws IOException {
    File tsvFolder = temporaryFolder.newFolder("data");
    new File(tsvFolder, ".hidden").createNewFile();
    File tsvFileX = new File(tsvFolder, "X");
    File tsvFileY = new File(tsvFolder, "Y");

    FileUtils.writeStringToFile(tsvFileX, "1\thello\n", Charset.forName("UTF-8"));
    FileUtils.writeStringToFile(tsvFileY, "2\taloha\n", Charset.forName("UTF-8"));

    cascading.tap.local.PartitionTap partitionTap = new cascading.tap.local.PartitionTap(
        new cascading.tap.local.FileTap(new cascading.scheme.local.TextDelimited(valueFields),
            tsvFolder.getAbsolutePath()), new DelimitedPartition(partitionFields));

    Data actual = new TapDataReader(partitionTap).read();

    assertThat(actual.orderBy(fields).asTupleEntryList(), is(expected.orderBy(fields).asTupleEntryList()));
  }

  @Test
  public void readHadoop() throws IOException {
    File tsvFolder = temporaryFolder.newFolder("data");
    File tsvFile = new File(tsvFolder, "part-00000");

    FileUtils.writeStringToFile(tsvFile, "1\thello\tX\n2\taloha\tY\n", Charset.forName("UTF-8"));

    cascading.tap.hadoop.Hfs hfs = new cascading.tap.hadoop.Hfs(new cascading.scheme.hadoop.TextDelimited(fields),
        tsvFolder.getAbsolutePath());

    Data actual = new TapDataReader(hfs).read();

    assertThat(actual, is(expected));
  }

  @Test
  public void readMultiSource() throws IOException {
    File tsvFolder = temporaryFolder.newFolder("data");
    File tsvFile = new File(tsvFolder, "part-00000");

    FileUtils.writeStringToFile(tsvFile, "1\thello\tX\n2\taloha\tY\n", Charset.forName("UTF-8"));

    cascading.tap.MultiSourceTap<?, ?, ?> multiTap = new cascading.tap.MultiSourceTap<>(new cascading.tap.hadoop.Hfs(
        new cascading.scheme.hadoop.TextDelimited(fields), tsvFolder.getAbsolutePath()));

    Data actual = new TapDataReader(multiTap).read();

    assertThat(actual, is(expected));
  }

  @Test
  public void readHadoopPartitions() throws IOException {
    File tsvFolder = temporaryFolder.newFolder("data");
    File tsvFileX = new File(new File(tsvFolder, "X"), "part-00000-00000");
    File tsvFileY = new File(new File(tsvFolder, "Y"), "part-00000-00000");

    FileUtils.writeStringToFile(tsvFileX, "1\thello\n", Charset.forName("UTF-8"));
    FileUtils.writeStringToFile(tsvFileY, "2\taloha\n", Charset.forName("UTF-8"));

    cascading.tap.hadoop.PartitionTap partitionTap = new cascading.tap.hadoop.PartitionTap(
        new cascading.tap.hadoop.Hfs(new cascading.scheme.hadoop.TextDelimited(valueFields),
            tsvFolder.getAbsolutePath()), new DelimitedPartition(partitionFields));

    Data actual = new TapDataReader(partitionTap).read();

    assertThat(actual.orderBy(fields).asTupleEntryList(), is(expected.orderBy(fields).asTupleEntryList()));
  }

  @Test
  public void readHadoopPartitionsWithHiddenFile() throws IOException {
    File tsvFolder = temporaryFolder.newFolder("data");
    new File(tsvFolder, ".hidden").createNewFile();
    File tsvFileX = new File(new File(tsvFolder, "X"), "part-00000-00000");
    File tsvFileY = new File(new File(tsvFolder, "Y"), "part-00000-00000");

    FileUtils.writeStringToFile(tsvFileX, "1\thello\n", Charset.forName("UTF-8"));
    FileUtils.writeStringToFile(tsvFileY, "2\taloha\n", Charset.forName("UTF-8"));

    cascading.tap.hadoop.PartitionTap partitionTap = new cascading.tap.hadoop.PartitionTap(
        new cascading.tap.hadoop.Hfs(new cascading.scheme.hadoop.TextDelimited(valueFields),
            tsvFolder.getAbsolutePath()), new DelimitedPartition(partitionFields));

    Data actual = new TapDataReader(partitionTap).read();

    assertThat(actual.orderBy(fields).asTupleEntryList(), is(expected.orderBy(fields).asTupleEntryList()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void unsupportedTap() throws IOException {
    new TapDataReader(new UnsupportedTap()).read();
  }

  @Test
  public void tupleEntryIteratorIsClosed() throws IOException {
    cascading.tap.hadoop.Hfs hfs = mock(cascading.tap.hadoop.Hfs.class);
    @SuppressWarnings("unchecked")
    cascading.flow.FlowProcess<Configuration> flowProcess = any(cascading.flow.FlowProcess.class);
    cascading.tuple.TupleEntryIterator iterator = mock(cascading.tuple.TupleEntryIterator.class);

    when(hfs.openForRead(flowProcess)).thenReturn(iterator);

    new TapDataReader(hfs).read();

    verify(iterator).close();
  }

}
