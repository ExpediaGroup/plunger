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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class PrettyPrinter {

  private boolean header = true;
  private final Data result;

  PrettyPrinter(Data result) {
    this.result = result;
  }

  /** Instructs the {@link PrettyPrinter} to omit the fields header */
  public PrettyPrinter noHeader() {
    header = false;
    return this;
  }

  /**
   * Prints the captured {@link Tuple Tuples} to {@link System#out} as a tab delimited table, using the derived
   * {@link Fields} as a header row.
   */
  public void print() {
    printTo(System.out);
  }

  /**
   * Prints the captured {@link Tuple Tuples} to the provided {@link PrintStream} as a tab delimited table, using the
   * derived {@link Fields} as a header row.
   */
  public void printTo(PrintStream stream) {
    if (header) {
      join(result.selectedFields(), '\t', stream);
      stream.append('\n');
    }
    List<Tuple> tuples = result.asTupleList();
    for (Tuple tuple : tuples) {
      join(tuple, '\t', stream);
      stream.append('\n');
    }
    stream.flush();
  }

  /** Prints the captured {@link Tuple Tuples} to a string with the UTF-8 encoding */
  @Override
  public String toString() {
    try {
      return toString("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  /** Prints the captured {@link Tuple Tuples} to a string with the provided encoding */
  public String toString(String encoding) throws UnsupportedEncodingException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try {
      printTo(new PrintStream(bytes));
    } finally {
      try {
        bytes.close();
      } catch (IOException ignored) {
      }
    }
    return bytes.toString(encoding);
  }

  private void join(Iterable<?> iterable, char joinChar, PrintStream stream) {
    boolean firstField = true;
    for (Object value : iterable) {
      if (firstField) {
        firstField = false;
      } else {
        stream.append(joinChar);
      }
      stream.print(value);
    }
  }

}
