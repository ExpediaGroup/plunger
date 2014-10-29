package com.hotels.plunger.asserts;

import static java.lang.Math.min;
import static java.lang.String.format;

import java.util.List;

import org.hamcrest.Description;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import cascading.tuple.TupleEntry;

class TupleEntryListMatcher extends TypeSafeDiagnosingMatcher<List<TupleEntry>> {
  private final List<TupleEntry> expected;

  TupleEntryListMatcher(List<TupleEntry> expected) {
    if (expected == null) {
      throw new IllegalArgumentException(
          "expected cannot be null. Consider using org.hamcrest.CoreMatchers.nullValue() instead.");
    }
    this.expected = expected;
  }

  @Override
  public void describeTo(Description description) {
    description.appendText("TupleEntry Lists should be identical");
  }

  @Override
  protected boolean matchesSafely(List<TupleEntry> actual, Description description) {
    if (actual == null) {
      description.appendText("The item is null");
      return false;
    }
    boolean result = true;
    if (actual.size() != expected.size()) {
      description.appendText(format("Expected size was %s, but was %s%n", expected.size(), actual.size()));
      result = false;
    }
    int size = min(actual.size(), expected.size());
    for (int i = 0; i < size; i++) {
      Description stringDescription = new StringDescription();
      if (!new TupleEntryMatcher(expected.get(i)).matchesSafely(actual.get(i), stringDescription)) {
        description.appendText(format("Items at index %s do not match:%n", i));
        description.appendText(stringDescription.toString());
        result = false;
      }
    }
    return result;
  }
}