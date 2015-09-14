/**
 * Copyright (C) 2015 Expedia Inc.
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
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Rename;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import org.junit.Test;

import java.util.List;

public class SortTest {

  private static class SortAssembly extends SubAssembly {
    public SortAssembly(Pipe pipe) {
      super(pipe);

      // Sort by S
      pipe = new GroupBy(pipe, FIELD_S);

      // Rename x -> v. Note that [S, x, y] becomes [S, y, v]
      pipe = new Rename(pipe, FIELD_X, FIELD_V);
      setTails(pipe);
    }

  }


  private static FlowDef defineFlow(Tap in, Tap out) {
    Pipe pipe = new Pipe("pipe");
    pipe = new SortAssembly(pipe);
    return FlowDef.flowDef().addSource(pipe, in).addTailSink(pipe, out);
  }

  private static Fields newStringFields(String fields) {
    return new Fields(fields, String.class);
  }

  private static final Fields FIELD_S = newStringFields("S");
  private static final Fields FIELD_X = newStringFields("x");
  private static final Fields FIELD_Y = newStringFields("y");
  private static final Fields FIELD_V = newStringFields("v");


  @Test
  public void testComplete() throws Exception {

    Bucket sink = new Bucket();

    Fields inFields = Fields.join(FIELD_S, FIELD_X, FIELD_Y);

    TupleListTap source = new DataBuilder(inFields)
        .addTuple("A", "a", "za")
        .addTuple("B", "b", "zb")
        .addTuple("AA", "aa", "zaa")
        .addTuple("BB", "bb", "zbb")
        .toTap();

    FlowDef flowDef = defineFlow(source, sink);

    new LocalFlowConnector().connect(flowDef).complete();

    List<TupleEntry> tupleEntries = sink.result().asTupleEntryList();

    assertThat(tupleEntries.get(0).getString(FIELD_S), is("A"));
    assertThat(tupleEntries.get(0).getString(FIELD_Y), is("za"));
    assertThat(tupleEntries.get(0).getString(FIELD_V), is("a"));
    assertThat(tupleEntries.get(1).getString(FIELD_S), is("AA"));
    assertThat(tupleEntries.get(1).getString(FIELD_Y), is("zaa"));
    assertThat(tupleEntries.get(1).getString(FIELD_V), is("aa"));
    assertThat(tupleEntries.get(2).getString(FIELD_S), is("B"));
    assertThat(tupleEntries.get(3).getString(FIELD_S), is("BB"));
    assertThat(tupleEntries.get(3).getString(FIELD_Y), is("zbb"));
    assertThat(tupleEntries.get(3).getString(FIELD_V), is("bb"));

  }

}
