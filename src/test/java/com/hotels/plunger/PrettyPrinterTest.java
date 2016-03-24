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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.hotels.plunger.Data;
import com.hotels.plunger.PrettyPrinter;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class PrettyPrinterTest {

  private final List<Tuple> tuples = new ArrayList<Tuple>();
  private Data data;

  @Before
  public void setup() {
    tuples.add(new Tuple(1, 100));
    tuples.add(new Tuple(2, 200));
    data = new Data(new Fields("A", "B"), tuples);
  }

  @Test
  public void printToDefaultHeader() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(bytes);
    new PrettyPrinter(data).printTo(printStream);

    printStream.flush();
    printStream.close();

    assertThat(bytes.toString("UTF-8"), is("A\tB\n1\t100\n2\t200\n"));
  }

  @Test
  public void printToNoHeader() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(bytes);
    new PrettyPrinter(data).noHeader().printTo(printStream);

    printStream.flush();
    printStream.close();

    assertThat(bytes.toString("UTF-8"), is("1\t100\n2\t200\n"));
  }

}
