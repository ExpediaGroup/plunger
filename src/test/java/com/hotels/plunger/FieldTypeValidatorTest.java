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

import org.junit.Test;

import cascading.tuple.Fields;

public class FieldTypeValidatorTest {

  @Test
  public void validateNullValue() {
    Fields input = new Fields("foo", String.class);
    Object[] result = FieldTypeValidator.validateValues(input, null);
    assertThat(result, is(new Object[1]));
  }

  @Test
  public void validateEmptyValue() {
    Fields input = new Fields("foo", String.class);
    Object[] result = FieldTypeValidator.validateValues(input, new Object[0]);
    assertThat(result, is(new Object[1]));
  }

  @Test
  public void validateNoValue() {
    Fields input = new Fields("foo", String.class);
    Object[] result = FieldTypeValidator.validateValues(input);
    assertThat(result, is(new Object[1]));
  }

  @Test(expected = IllegalStateException.class)
  public void validateIncorrectValueLength() {
    Fields input = new Fields("foo", String.class);
    FieldTypeValidator.validateValues(input, "first", "second");
  }

  @Test
  public void validateObjectType() {
    Fields input = new Fields("foo", String.class);
    Object[] result = FieldTypeValidator.validateValues(input, "someValue");
    assertThat(result, is(new Object[] { "someValue" }));
  }

  @Test
  public void validateNoType() {
    Fields input = new Fields("foo");
    Object[] result = FieldTypeValidator.validateValues(input, "someValue");
    assertThat(result, is(new Object[] { "someValue" }));
  }

  @Test
  public void validateObjectIncorrectType() {
    Fields input = new Fields("foo", String.class);
    // we intentionally don't cover this case as Cascading will throw an error when you try create a Tuple like this
    Object[] result = FieldTypeValidator.validateValues(input, new Integer(1));
    assertThat(result, is(new Object[] { new Integer(1) }));
  }

  @Test(expected = IllegalStateException.class)
  public void validateNullPrimitive() {
    Fields input = new Fields("foo", boolean.class);
    FieldTypeValidator.validateValues(input, null);
  }

  @Test(expected = IllegalStateException.class)
  public void validateInvalidPrimitive() {
    Fields input = new Fields("foo", boolean.class);
    FieldTypeValidator.validateValues(input, 4);
  }

  @Test(expected = IllegalStateException.class)
  public void validateNotAssignablePrimitiveIntToLong() {
    Fields input = new Fields("foo", long.class);
    FieldTypeValidator.validateValues(input, 4);
  }

  @Test(expected = IllegalStateException.class)
  public void validateNotAssignablePrimitiveLongToInt() {
    Fields input = new Fields("foo", int.class);
    FieldTypeValidator.validateValues(input, 4L);
  }

  @Test(expected = IllegalStateException.class)
  public void validateObjectAsPrimitive() {
    Fields input = new Fields("foo", boolean.class);
    FieldTypeValidator.validateValues(input, "someValue");
  }

}
