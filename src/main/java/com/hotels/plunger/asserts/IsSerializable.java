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

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Is the value serializable?
 */
public class IsSerializable<T> extends BaseMatcher<T> {

  private String errorMessage;
  private Object value;

  @Override
  public boolean matches(Object value) {
    this.value = value;
    errorMessage = getSerializationError(value);
    return errorMessage == null;
  }

  @Override
  public void describeTo(Description description) {
    description.appendText(value.getClass().getName()).appendText(" to be serializable");
  }

  @Override
  public void describeMismatch(Object item, Description description) {
    description.appendText("failed to serialize ").appendText(errorMessage);
  }

  /**
   * Matches if value is serializable.
   */
  @Factory
  public static <T> Matcher<T> serializable() {
    return new IsSerializable<T>();
  }

  /**
   * Matches if value is serializable. With type inference.
   */
  @Factory
  public static <T> Matcher<T> serializable(Class<T> type) {
    return serializable();
  }

  static String getSerializationError(Object value) {
    ObjectOutputStream outputStream = null;
    try {
      outputStream = new ObjectOutputStream(NULL_OUTPUT_STREAM);
      outputStream.writeObject(value);
      return null;
    } catch (IOException e) {
      return e.getMessage();
    } finally {
      if (outputStream != null) {
        try {
          outputStream.close();
        } catch (IOException ignored) {
        }
      }
    }
  }

  static final OutputStream NULL_OUTPUT_STREAM = new OutputStream() {
    /** Discards the specified byte. */
    @Override
    public void write(int b) {
    }

    /** Discards the specified byte array. */
    @Override
    public void write(byte[] b) {
      if (b == null) {
        throw new NullPointerException();
      }
    }

    /** Discards the specified byte array. */
    @Override
    public void write(byte[] b, int off, int len) {
      if (b == null) {
        throw new NullPointerException();
      }
    }

    @Override
    public String toString() {
      return "NULL_OUTPUT_STREAM";
    }
  };

}