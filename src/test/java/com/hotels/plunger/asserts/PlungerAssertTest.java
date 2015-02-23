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
package com.hotels.plunger.asserts;

import static com.hotels.plunger.asserts.PlungerAssert.serializable;
import static com.hotels.plunger.asserts.PlungerAssert.tupleEntry;
import static com.hotels.plunger.asserts.PlungerAssert.tupleEntryList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;

import com.hotels.plunger.DataBuilder;
import com.hotels.plunger.asserts.PlungerAssert;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class PlungerAssertTest {

  private static final Fields FIELDS_A_STRING = new Fields("A", String.class);
  private static final Fields FIELDS_A_INT = new Fields("A", Integer.class);
  private static final Fields FIELDS_AB_STRING = new Fields(new String[] { "A", "B" }, new Class<?>[] { String.class,
      String.class });
  private static final Fields FIELDS_BA_STRING = new Fields(new String[] { "B", "A" }, new Class<?>[] { String.class,
      String.class });

  @Test
  public void assertSerializable() {
    PlungerAssert.assertSerializable("STRING");
  }

  @Test
  public void assertThatIsSerializable() {
    assertThat("STRING", is(serializable()));
  }

  @Test(expected = AssertionError.class)
  public void assertSerializableFails() {
    PlungerAssert.assertSerializable(new PlungerAssertTest());
  }

  @Test(expected = AssertionError.class)
  public void assertThatIsSerializableFails() {
    assertThat(new PlungerAssertTest(), is(serializable()));
  }

  @Test
  public void assertIsTupleEntryMatches() {
    assertThat(entry(FIELDS_A_STRING, "a"), is(tupleEntry(FIELDS_A_STRING, "a")));
  }

  @Test(expected = AssertionError.class)
  public void assertIsTupleEntryMissing() {
    assertThat(entry(FIELDS_A_STRING, "a"), is(tupleEntry(FIELDS_AB_STRING, "a", "b")));
  }

  @Test(expected = AssertionError.class)
  public void assertIsTupleEntryExtra() {
    assertThat(entry(FIELDS_AB_STRING, "a", "b"), is(tupleEntry(FIELDS_A_STRING, "a")));
  }

  @Test(expected = AssertionError.class)
  public void assertIsTupleEntryDifferentType() {
    assertThat(entry(FIELDS_A_STRING, "a"), is(tupleEntry(FIELDS_A_INT, 1)));
  }

  @Test(expected = AssertionError.class)
  public void assertIsTupleEntryDifferentPosition() {
    assertThat(entry(FIELDS_AB_STRING, "a", "b"), is(tupleEntry(FIELDS_BA_STRING, "b", "a")));
  }

  @Test(expected = AssertionError.class)
  public void assertIsTupleEntryDifferentValue() {
    assertThat(entry(FIELDS_A_STRING, "a"), is(tupleEntry(FIELDS_A_STRING, "b")));
  }

  @Test
  public void assertIsTupleEntryListMatches() {
    List<TupleEntry> actual = new DataBuilder(FIELDS_A_STRING).addTuple("a").build().asTupleEntryList();
    List<TupleEntry> expected = new DataBuilder(FIELDS_A_STRING).addTuple("a").build().asTupleEntryList();
    assertThat(actual, is(tupleEntryList(expected)));
  }

  @Test(expected = AssertionError.class)
  public void assertIsTupleEntryListSizeDifferent() {
    List<TupleEntry> actual = new DataBuilder(FIELDS_A_STRING).addTuple("a").build().asTupleEntryList();
    List<TupleEntry> expected = new DataBuilder(FIELDS_A_STRING).build().asTupleEntryList();
    assertThat(actual, is(tupleEntryList(expected)));
  }

  @Test(expected = AssertionError.class)
  public void assertIsTupleEntryListEntryMissing() {
    List<TupleEntry> actual = new DataBuilder(FIELDS_A_STRING).addTuple("a").build().asTupleEntryList();
    List<TupleEntry> expected = new DataBuilder(FIELDS_AB_STRING).addTuple("a", "b").build().asTupleEntryList();
    assertThat(actual, is(tupleEntryList(expected)));
  }

  private static TupleEntry entry(Fields fields, Object... values) {
    return new TupleEntry(fields, new Tuple(values));
  }

}
