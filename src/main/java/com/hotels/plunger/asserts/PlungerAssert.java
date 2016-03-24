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
package com.hotels.plunger.asserts;

import static org.junit.Assert.fail;

import java.util.List;

import org.hamcrest.Matcher;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public final class PlungerAssert {

  private PlungerAssert() {
  }

  public static void assertSerializable(Object value) {
    String serializationError = IsSerializable.getSerializationError(value);
    if (serializationError == null) {
      return;
    }
    fail(serializationError);
  }

  public static <T> Matcher<T> serializable() {
    return IsSerializable.serializable();
  }

  public static Matcher<TupleEntry> tupleEntry(TupleEntry expected) {
    return new TupleEntryMatcher(expected);
  }

  public static Matcher<TupleEntry> tupleEntry(Fields fields, Object... values) {
    return tupleEntry(fields, new Tuple(values));
  }

  public static Matcher<TupleEntry> tupleEntry(Fields fields, Tuple tuple) {
    if (fields.size() != tuple.size()) {
      throw new IllegalArgumentException("Fields size (" + fields.size() + ") does not match tuple size ("
          + tuple.size() + ")");
    }
    return tupleEntry(new TupleEntry(fields, tuple));
  }

  public static TupleEntryListMatcher tupleEntryList(List<TupleEntry> expected) {
    return new TupleEntryListMatcher(expected);
  }

}
