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

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Test;

import cascading.scheme.Scheme;
import cascading.tap.partition.DelimitedPartition;
import cascading.tuple.Fields;

public class TapTypeUtilTest {

  @Test
  public void localFileTap() {
    Class<?> tapType = TapTypeUtil.getTapConfigClass(new cascading.tap.local.FileTap(
        new cascading.scheme.local.TextDelimited(), ""));
    assertEquals(Properties.class, tapType);
  }

  @Test
  public void localPartitionTap() {
    Class<?> tapType = TapTypeUtil.getTapConfigClass(new cascading.tap.local.PartitionTap(
        new cascading.tap.local.FileTap(new cascading.scheme.local.TextDelimited(), ""), new DelimitedPartition(
            new Fields("A"))));
    assertEquals(Properties.class, tapType);
  }

  @Test
  public void hadoopHfsTap() {
    Class<?> tapType = TapTypeUtil.getTapConfigClass(new cascading.tap.hadoop.Hfs(
        new cascading.scheme.hadoop.TextDelimited(), ""));
    assertEquals(Configuration.class, tapType);
  }

  @Test
  public void multiLevelHierarchy() {
    Class<?> tapType = TapTypeUtil.getTapConfigClass(new TestHfs(new cascading.scheme.hadoop.TextDelimited(), ""));
    assertEquals(Configuration.class, tapType);
  }

  @Test
  public void hadoopPartitionTap() {
    Class<?> tapType = TapTypeUtil.getTapConfigClass(new cascading.tap.hadoop.PartitionTap(
        new cascading.tap.hadoop.Hfs(new cascading.scheme.hadoop.TextDelimited(), ""), new DelimitedPartition(
            new Fields("A"))));
    assertEquals(Configuration.class, tapType);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void compositeHadoopTap() {
    Class<?> tapType = TapTypeUtil.getTapConfigClass(new cascading.tap.MultiSourceTap(new cascading.tap.hadoop.Hfs(
        new cascading.scheme.hadoop.TextDelimited(), "")));
    assertEquals(Configuration.class, tapType);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void compositeLocalTap() {
    Class<?> tapType = TapTypeUtil.getTapConfigClass(new cascading.tap.MultiSourceTap(new cascading.tap.local.FileTap(
        new cascading.scheme.local.TextDelimited(), "")));
    assertEquals(Properties.class, tapType);
  }

  private static class TestHfs extends cascading.tap.hadoop.Hfs {

    private static final long serialVersionUID = 1L;

    private TestHfs(@SuppressWarnings("rawtypes") Scheme<Configuration, RecordReader, OutputCollector, ?, ?> scheme,
        String stringPath) {
      super(scheme, stringPath);
    }
  }

}
