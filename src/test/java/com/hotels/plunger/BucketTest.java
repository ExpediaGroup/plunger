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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.plunger.Bucket;
import com.hotels.plunger.PlungerFlow;

import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

@RunWith(MockitoJUnitRunner.class)
public class BucketTest {

  private static final Fields FIELDS = new Fields("A", "B");
  private static final Tuple TUPLE_1 = new Tuple(1, "x");
  private static final Tuple TUPLE_2 = new Tuple(2, "y");

  @Mock
  private PlungerFlow flow;
  @Mock
  private FlowDef flowDef;
  @Mock
  private Pipe pipe;

  @Before
  public void setup() {
    when(flow.isComplete()).thenReturn(false);
    when(flow.getFlowDef()).thenReturn(flowDef);
  }

  @Test
  public void addsToFlowDef() throws IOException {
    Bucket sink = new Bucket(FIELDS, pipe, flow);
    verify(flowDef).addTailSink(pipe, sink);
  }

  @Test(expected = IllegalStateException.class)
  public void failsIfAlreadyComplete() throws IOException {
    when(flow.isComplete()).thenReturn(true);
    new Bucket(FIELDS, pipe, flow);
  }

  @Test
  public void asTupleEntryList() throws IOException {
    Bucket sink = new Bucket(FIELDS, pipe, flow);
    TupleEntryCollector collector = sink.openForWrite(null, null);
    collector.add(TUPLE_1);
    collector.add(TUPLE_2);
    List<TupleEntry> tupleEntryList = sink.result().asTupleEntryList();
    assertThat(tupleEntryList.size(), is(2));
    assertThat(tupleEntryList.get(0).getFields(), is(FIELDS));
    assertThat(tupleEntryList.get(0).getTuple(), is(TUPLE_1));
    assertThat(tupleEntryList.get(1).getFields(), is(FIELDS));
    assertThat(tupleEntryList.get(1).getTuple(), is(TUPLE_2));
  }

  @Test
  public void asTupleList() throws IOException {
    Bucket sink = new Bucket(FIELDS, pipe, flow);
    TupleEntryCollector collector = sink.openForWrite(null, null);
    collector.add(TUPLE_1);
    collector.add(TUPLE_2);
    List<Tuple> tupleList = sink.result().asTupleList();
    assertThat(tupleList.size(), is(2));
    assertThat(tupleList.get(0), is(TUPLE_1));
    assertThat(tupleList.get(1), is(TUPLE_2));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void asTupleEntryListReturnsImmutable() throws IOException {
    Bucket sink = new Bucket(FIELDS, pipe, flow);
    sink.result().asTupleEntryList().add(new TupleEntry());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void asTupleListReturnsImmutable() throws IOException {
    Bucket sink = new Bucket(FIELDS, pipe, flow);
    sink.result().asTupleList().add(new Tuple());
  }

  @Test
  public void getIdentifier() {
    Bucket sink = new Bucket(FIELDS, pipe, flow);
    assertThat(sink.getIdentifier().startsWith(Bucket.class.getSimpleName()), is(true));
  }

  @Test
  public void createResource() throws IOException {
    Bucket sink = new Bucket(FIELDS, pipe, flow);
    assertThat(sink.resourceExists(new Properties()), is(false));
    assertThat(sink.createResource(new Properties()), is(true));
    assertThat(sink.resourceExists(new Properties()), is(true));
  }

  @Test
  public void deleteResource() throws IOException {
    Bucket sink = new Bucket(FIELDS, pipe, flow);
    assertThat(sink.deleteResource(new Properties()), is(true));
  }

  @Test
  public void modified() throws IOException, InterruptedException {
    Bucket sink = new Bucket(FIELDS, pipe, flow);
    long modifiedTime = sink.getModifiedTime(new Properties());
    Thread.sleep(10);
    long checkTime = System.currentTimeMillis();
    assertThat(modifiedTime < checkTime, is(true));
    Thread.sleep(10);
    sink.modified();
    Thread.sleep(10);
    modifiedTime = sink.getModifiedTime(new Properties());
    assertThat(modifiedTime > checkTime, is(true));
    assertThat(modifiedTime < System.currentTimeMillis(), is(true));
  }

}
