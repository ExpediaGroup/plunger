package com.hotels.plunger.asserts;

import static java.lang.String.format;

import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

class TupleEntryMatcher extends TypeSafeDiagnosingMatcher<TupleEntry> {
  private final TupleEntry expected;

  TupleEntryMatcher(TupleEntry expected) {
    if (expected == null) {
      throw new IllegalArgumentException(
          "expected cannot be null. Consider using org.hamcrest.CoreMatchers.nullValue() instead.");
    }
    this.expected = expected;
  }

  @Override
  public void describeTo(Description description) {
    description.appendText("TupleEntries should be identical");
  }

  @Override
  protected boolean matchesSafely(TupleEntry actual, Description description) {
    Set<Comparable<?>> actualNames = extractFieldNames(actual.getFields());
    Set<Comparable<?>> expectedNames = extractFieldNames(expected.getFields());

    Set<Comparable<?>> allNames = new HashSet<Comparable<?>>();
    allNames.addAll(actualNames);
    allNames.addAll(expectedNames);

    boolean result = true;
    for (Comparable<?> name : allNames) {
      Fields field = new Fields(name);
      if (!actual.getFields().contains(field)) {
        description.appendText(format("%s was expected, but was not present%n", name));
        result = false;
        continue;
      }
      if (!expected.getFields().contains(field)) {
        description.appendText(format("%s was not expected, but was present%n", name));
        result = false;
        continue;
      }
      int actualPos = actual.getFields().getPos(name);
      int expectedPos = expected.getFields().getPos(name);
      if (actualPos != expectedPos) {
        description.appendText(format("%s expected position was %s, but was %s%n", name, expectedPos, actualPos));
        result = false;
      }
      Type actualType = actual.getFields().getType(name);
      Type expectedType = expected.getFields().getType(name);
      if (!equal(actualType, expectedType)) {
        description.appendText(format("%s expected type was %s, but was %s%n", name, expectedType, actualType));
        result = false;
        continue;
      }
      Object actualValue = actual.getObject(name);
      Object expectedValue = expected.getObject(name);
      if (!equal(actualValue, expectedValue)) {
        description.appendText(format("%s expected value was '%s', but was '%s'%n", name, expectedValue, actualValue));
        result = false;
      }
    }
    return result;
  }

  private static boolean equal(Object o1, Object o2) {
    return o1 == o2 || o1 != null && o1.equals(o2);
  }

  private static Set<Comparable<?>> extractFieldNames(Fields fields) {
    Set<Comparable<?>> names = new HashSet<Comparable<?>>();
    @SuppressWarnings("unchecked")
    Iterator<Comparable<?>> iterator = fields.iterator();
    while (iterator.hasNext()) {
      names.add(iterator.next());
    }
    return names;
  }
}