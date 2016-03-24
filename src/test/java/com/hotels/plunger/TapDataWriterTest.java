/**
 * Copyright (C) 2014-2016 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import cascading.tap.Tap;
import cascading.tap.hadoop.util.Hadoop18TapUtil;
import cascading.tap.partition.DelimitedPartition;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class TapDataWriterTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final Fields partitionFields = new Fields("A", String.class);
  private final Fields valueFields = new Fields(Fields.names("B", "C"), Fields.types(Integer.TYPE, String.class));
  private final Fields fields = Fields.join(partitionFields, valueFields);

  private final Data data = new Data(fields, Arrays.asList(new Tuple("X", 1, "hello"), new Tuple("Y", 2, "world")));

  @Test
  public void writeLocal() throws IOException {
    File tsvFile = temporaryFolder.newFile("data.tsv");
    cascading.tap.local.FileTap fileTap = new cascading.tap.local.FileTap(new cascading.scheme.local.TextDelimited(),
        tsvFile.getAbsolutePath());
    Tap<?, ?, ?> returnedTap = new TapDataWriter(data).toTap(fileTap);

    assertThat((cascading.tap.local.FileTap) returnedTap, is(fileTap));
    String written = FileUtils.readFileToString(tsvFile, Charset.forName("UTF-8"));

    assertThat(written, is("X\t1\thello\nY\t2\tworld\n"));
  }

  @Test
  public void writeLocalPartition() throws IOException {
    File tsvFolder = temporaryFolder.newFolder("data");
    cascading.tap.local.PartitionTap partitionTap = new cascading.tap.local.PartitionTap(
        new cascading.tap.local.FileTap(new cascading.scheme.local.TextDelimited(valueFields),
            tsvFolder.getAbsolutePath()), new DelimitedPartition(partitionFields));
    Tap<?, ?, ?> returnedTap = new TapDataWriter(data).toTap(partitionTap);

    assertThat((cascading.tap.local.PartitionTap) returnedTap, is(partitionTap));

    File tsvFileX = new File(tsvFolder, "X");
    String writtenX = FileUtils.readFileToString(tsvFileX, Charset.forName("UTF-8"));

    assertThat(writtenX, is("1\thello\n"));

    File tsvFileY = new File(tsvFolder, "Y");
    String writtenY = FileUtils.readFileToString(tsvFileY, Charset.forName("UTF-8"));

    assertThat(writtenY, is("2\tworld\n"));
  }

  @Test
  public void writeHfs() throws IOException {
    File tsvFolder = temporaryFolder.newFolder("data");
    cascading.tap.hadoop.Hfs hfsTap = new cascading.tap.hadoop.Hfs(new cascading.scheme.hadoop.TextDelimited(),
        tsvFolder.getAbsolutePath());
    Tap<?, ?, ?> returnedTap = new TapDataWriter(data).toTap(hfsTap);

    assertThat((cascading.tap.hadoop.Hfs) returnedTap, is(hfsTap));
    String written = FileUtils.readFileToString(new File(tsvFolder, "part-00000"), Charset.forName("UTF-8"));

    assertThat(written, is("X\t1\thello\nY\t2\tworld\n"));
    assertThat(new File(tsvFolder, Hadoop18TapUtil.TEMPORARY_PATH).exists(), is(false));
  }

  @Test
  public void writeHadoopPartition() throws IOException {
    File tsvFolder = temporaryFolder.newFolder("data");
    cascading.tap.hadoop.PartitionTap partitionTap = new cascading.tap.hadoop.PartitionTap(
        new cascading.tap.hadoop.Hfs(new cascading.scheme.hadoop.TextDelimited(valueFields),
            tsvFolder.getAbsolutePath()), new DelimitedPartition(partitionFields));

    Data data = new Data(fields, Arrays.asList(new Tuple("X", 1, "hello"), new Tuple("Y", 2, "world")));
    Tap<?, ?, ?> returnedTap = new TapDataWriter(data).toTap(partitionTap);

    assertThat((cascading.tap.hadoop.PartitionTap) returnedTap, is(partitionTap));

    File tsvFileX = new File(new File(tsvFolder, "X"), "part-00000-00000");
    String writtenX = FileUtils.readFileToString(tsvFileX, Charset.forName("UTF-8"));

    assertThat(writtenX, is("1\thello\n"));

    File tsvFileY = new File(new File(tsvFolder, "Y"), "part-00000-00001");
    String writtenY = FileUtils.readFileToString(tsvFileY, Charset.forName("UTF-8"));

    assertThat(writtenY, is("2\tworld\n"));

    assertThat(new File(tsvFolder, Hadoop18TapUtil.TEMPORARY_PATH).exists(), is(false));
  }

  @Test
  public void writeMultiSink() throws IOException {
    File tsvFolder1 = temporaryFolder.newFolder("data1");
    File tsvFolder2 = temporaryFolder.newFolder("data2");

    Tap<?, ?, ?> tap1 = new cascading.tap.hadoop.Hfs(new cascading.scheme.hadoop.TextDelimited(fields),
        tsvFolder1.getAbsolutePath());
    Tap<?, ?, ?> tap2 = new cascading.tap.hadoop.Hfs(new cascading.scheme.hadoop.TextDelimited(valueFields),
        tsvFolder2.getAbsolutePath());

    @SuppressWarnings("unchecked")
    cascading.tap.MultiSinkTap<?, ?, ?> multiTap = new cascading.tap.MultiSinkTap<>(tap1, tap2);
    Tap<?, ?, ?> returnedTap = new TapDataWriter(data).toTap(multiTap);

    assertThat(returnedTap == multiTap, is(true));

    String written1 = FileUtils.readFileToString(new File(tsvFolder1, "part-00000"), Charset.forName("UTF-8"));
    assertThat(written1, is("X\t1\thello\nY\t2\tworld\n"));

    String written2 = FileUtils.readFileToString(new File(tsvFolder2, "part-00000"), Charset.forName("UTF-8"));
    assertThat(written2, is("1\thello\n2\tworld\n"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void unsupportedTap() throws IOException {
    new TapDataWriter(data).toTap(new UnsupportedTap());
  }

}
