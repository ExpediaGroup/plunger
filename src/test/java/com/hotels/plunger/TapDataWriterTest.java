/**
 * Copyright 2014 Expedia Inc.
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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.hotels.plunger.Data;
import com.hotels.plunger.TapDataWriter;

import cascading.tap.Tap;
import cascading.tap.partition.DelimitedPartition;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class TapDataWriterTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final Fields partitionFields = new Fields("A", String.class);
  private final Fields valueFields = new Fields(Fields.names("B", "C"), Fields.types(Integer.TYPE, String.class));
  private final Fields fields = Fields.join(partitionFields, valueFields);

  private final Data data = new Data(fields, Arrays.asList(new Tuple("X", 1, "hello")));

  @Test
  public void writeLocal() throws IOException {
    File tsvFile = temporaryFolder.newFile("data.tsv");
    cascading.tap.local.FileTap fileTap = new cascading.tap.local.FileTap(new cascading.scheme.local.TextDelimited(),
        tsvFile.getAbsolutePath());
    Tap<?, ?, ?> returnedTap = new TapDataWriter(data).toTap(fileTap);

    assertThat((cascading.tap.local.FileTap) returnedTap, is(fileTap));
    String written = FileUtils.readFileToString(tsvFile, Charset.forName("UTF-8"));

    assertThat(written, is("X\t1\thello\n"));
  }

  @Test
  public void writeLocalPartition() throws IOException {
    File tsvFolder = temporaryFolder.newFolder("data");
    cascading.tap.local.PartitionTap partitionTap = new cascading.tap.local.PartitionTap(
        new cascading.tap.local.FileTap(new cascading.scheme.local.TextDelimited(valueFields),
            tsvFolder.getAbsolutePath()), new DelimitedPartition(partitionFields));
    Tap<?, ?, ?> returnedTap = new TapDataWriter(data).toTap(partitionTap);

    assertThat((cascading.tap.local.PartitionTap) returnedTap, is(partitionTap));

    File tsvFile = new File(tsvFolder, "X");
    String written = FileUtils.readFileToString(tsvFile, Charset.forName("UTF-8"));

    assertThat(written, is("1\thello\n"));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void writeLocalTemplate() throws IOException {
    File tsvFolder = temporaryFolder.newFolder("data");
    cascading.tap.local.TemplateTap templateTap = new cascading.tap.local.TemplateTap(new cascading.tap.local.FileTap(
        new cascading.scheme.local.TextDelimited(valueFields), tsvFolder.getAbsolutePath()), "%s", partitionFields);
    Tap<?, ?, ?> returnedTap = new TapDataWriter(data).toTap(templateTap);

    assertThat((cascading.tap.local.TemplateTap) returnedTap, is(templateTap));

    File tsvFile = new File(tsvFolder, "X");
    String written = FileUtils.readFileToString(tsvFile, Charset.forName("UTF-8"));

    assertThat(written, is("1\thello\n"));
  }

  @Test
  public void writeHfs() throws IOException {
    File tsvFolder = temporaryFolder.newFolder("data");
    cascading.tap.hadoop.Hfs hfsTap = new cascading.tap.hadoop.Hfs(new cascading.scheme.hadoop.TextDelimited(),
        tsvFolder.getAbsolutePath());
    Tap<?, ?, ?> returnedTap = new TapDataWriter(data).toTap(hfsTap);

    assertThat((cascading.tap.hadoop.Hfs) returnedTap, is(hfsTap));
    String written = FileUtils.readFileToString(new File(tsvFolder, "part-00000"), Charset.forName("UTF-8"));

    assertThat(written, is("X\t1\thello\n"));
  }

  @Test
  public void writeHadoopPartition() throws IOException {
    File tsvFolder = temporaryFolder.newFolder("data");
    cascading.tap.hadoop.PartitionTap partitionTap = new cascading.tap.hadoop.PartitionTap(
        new cascading.tap.hadoop.Hfs(new cascading.scheme.hadoop.TextDelimited(valueFields),
            tsvFolder.getAbsolutePath()), new DelimitedPartition(partitionFields));
    Tap<?, ?, ?> returnedTap = new TapDataWriter(data).toTap(partitionTap);

    assertThat((cascading.tap.hadoop.PartitionTap) returnedTap, is(partitionTap));

    File tsvFile = new File(new File(tsvFolder, "X"), "part-00000-00000");
    String written = FileUtils.readFileToString(tsvFile, Charset.forName("UTF-8"));

    assertThat(written, is("1\thello\n"));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void writeHadoopTemplate() throws IOException {
    File tsvFolder = temporaryFolder.newFolder("data");
    cascading.tap.hadoop.TemplateTap partitionTap = new cascading.tap.hadoop.TemplateTap(new cascading.tap.hadoop.Hfs(
        new cascading.scheme.hadoop.TextDelimited(valueFields), tsvFolder.getAbsolutePath()), "%s", partitionFields);
    Tap<?, ?, ?> returnedTap = new TapDataWriter(data).toTap(partitionTap);

    assertThat((cascading.tap.hadoop.TemplateTap) returnedTap, is(partitionTap));

    File tsvFile = new File(new File(tsvFolder, "X"), "part-00000");
    String written = FileUtils.readFileToString(tsvFile, Charset.forName("UTF-8"));

    assertThat(written, is("1\thello\n"));
  }

  @Test
  public void writeMultiSink() throws IOException {
    File tsvFolder = temporaryFolder.newFolder("data");
    cascading.tap.MultiSinkTap<?, ?, ?> multiTap = new cascading.tap.MultiSinkTap<>(new cascading.tap.hadoop.Hfs(
        new cascading.scheme.hadoop.TextDelimited(fields), tsvFolder.getAbsolutePath()));
    Tap<?, ?, ?> returnedTap = new TapDataWriter(data).toTap(multiTap);

    assertThat(returnedTap == multiTap, is(true));

    String written = FileUtils.readFileToString(new File(tsvFolder, "part-00000"), Charset.forName("UTF-8"));

    assertThat(written, is("X\t1\thello\n"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void unsupportedTap() throws IOException {
    new TapDataWriter(data).toTap(new UnsupportedTap());
  }

}
