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

import static com.hotels.plunger.asserts.PlungerAssert.serializable;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.inOrder;

import java.io.PrintStream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.plunger.Bucket;
import com.hotels.plunger.Data;
import com.hotels.plunger.DataBuilder;
import com.hotels.plunger.Dump;
import com.hotels.plunger.Plunger;
import com.hotels.plunger.Dump.PrintStreamSupplier;

import cascading.pipe.Pipe;
import cascading.tuple.Fields;

@RunWith(MockitoJUnitRunner.class)
public class DumpTest {

  private static final Fields FIELDS = new Fields(Fields.names("A", "B"), Fields.types(String.class, Integer.class));
  private static final Data INPUT_DATA = new DataBuilder(FIELDS).addTuple("one", 1).addTuple("two", 2).build();

  @Mock
  private PrintStream mockPrintStream;

  private final Plunger plunger = new Plunger();

  private PrintStreamSupplier printStreamSupplier;

  @Before
  public void setup() {
    printStreamSupplier = new Dump.PrintStreamSupplier() {
      @Override
      public PrintStream getPrintStream() {
        return mockPrintStream;
      }
    };
  }

  @Test
  public void typical() {
    Pipe dump = new Dump(plunger.newPipe(INPUT_DATA), printStreamSupplier);

    Bucket bucket = plunger.newBucket(FIELDS, dump);
    Data result = bucket.result();

    assertThat(result, is(INPUT_DATA));

    InOrder inOrder = inOrder(mockPrintStream);
    inOrder.verify(mockPrintStream).append("");
    inOrder.verify(mockPrintStream).append("A");
    inOrder.verify(mockPrintStream).append('\t');
    inOrder.verify(mockPrintStream).append("B");
    inOrder.verify(mockPrintStream).append('\n');
    inOrder.verify(mockPrintStream).append("");
    inOrder.verify(mockPrintStream).append("one");
    inOrder.verify(mockPrintStream).append('\t');
    inOrder.verify(mockPrintStream).append("1");
    inOrder.verify(mockPrintStream).append('\n');
    inOrder.verify(mockPrintStream).append("");
    inOrder.verify(mockPrintStream).append("two");
    inOrder.verify(mockPrintStream).append('\t');
    inOrder.verify(mockPrintStream).append("2");
    inOrder.verify(mockPrintStream).append('\n');
  }

  @Test
  public void prefix() {
    Pipe dump = new Dump("prefix", plunger.newPipe(INPUT_DATA), printStreamSupplier, FIELDS);

    Bucket bucket = plunger.newBucket(FIELDS, dump);
    Data result = bucket.result();

    assertThat(result, is(INPUT_DATA));

    InOrder inOrder = inOrder(mockPrintStream);
    inOrder.verify(mockPrintStream).append("prefix");
    inOrder.verify(mockPrintStream).append("A");
    inOrder.verify(mockPrintStream).append('\t');
    inOrder.verify(mockPrintStream).append("B");
    inOrder.verify(mockPrintStream).append('\n');
    inOrder.verify(mockPrintStream).append("prefix");
    inOrder.verify(mockPrintStream).append("one");
    inOrder.verify(mockPrintStream).append('\t');
    inOrder.verify(mockPrintStream).append("1");
    inOrder.verify(mockPrintStream).append('\n');
    inOrder.verify(mockPrintStream).append("prefix");
    inOrder.verify(mockPrintStream).append("two");
    inOrder.verify(mockPrintStream).append('\t');
    inOrder.verify(mockPrintStream).append("2");
    inOrder.verify(mockPrintStream).append('\n');
  }

  @Test
  public void fieldsOfInterest() {
    Fields fieldsOfInterest = new Fields("A", String.class);
    Pipe dump = new Dump(plunger.newPipe(INPUT_DATA), printStreamSupplier, fieldsOfInterest);

    Bucket bucket = plunger.newBucket(FIELDS, dump);
    Data result = bucket.result();

    assertThat(result, is(INPUT_DATA));

    InOrder inOrder = inOrder(mockPrintStream);
    inOrder.verify(mockPrintStream).append("");
    inOrder.verify(mockPrintStream).append("A");
    inOrder.verify(mockPrintStream).append('\n');
    inOrder.verify(mockPrintStream).append("");
    inOrder.verify(mockPrintStream).append("one");
    inOrder.verify(mockPrintStream).append('\n');
    inOrder.verify(mockPrintStream).append("");
    inOrder.verify(mockPrintStream).append("two");
    inOrder.verify(mockPrintStream).append('\n');
  }

  @Test
  public void serializes() {
    assertThat(new Dump(new Pipe("name")), is(serializable()));
  }

}
