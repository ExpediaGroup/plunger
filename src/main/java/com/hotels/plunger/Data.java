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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class Data {

  private final List<Tuple> tuples;
  private Fields sortFields;
  private final Fields declaredFields;
  private Fields withFields = Fields.ALL;

  Data(Fields declaredFields, List<Tuple> tuples) {
    this.declaredFields = declaredFields;
    this.tuples = tuples;
  }

  /**
   * Specifies that the returned results be ordered by the specified {@link Fields}. Assume natural ordering of the
   * input types.
   */
  public Data orderBy(Fields... fields) {
    if (fields != null && fields.length > 0) {
      sortFields = Fields.merge(fields);
    }
    return this;
  }

  /**
   * Specifies that the returned results be restricted to the specified {@link Fields}.
   */
  public Data withFields(Fields... fields) {
    if (fields != null && fields.length > 0) {
      for (Fields fieldsElement : fields) {
        // this check seems unnecessary, but Fields.merge() doesn't seem to handle this case
        if (fieldsElement == Fields.ALL) {
          withFields = Fields.ALL;
          return this;
        }
      }
      withFields = Fields.merge(fields);
    }
    return this;
  }

  /**
   * Returns the result as a {@link Tuple} list.
   */
  public List<Tuple> asTupleList() {
    List<Tuple> sorted = new ArrayList<Tuple>(tuples);
    if (sortFields != null && sortFields.size() > 0) {
      Collections.sort(sorted, new TupleComparator(declaredFields, sortFields));
    }
    Fields selectedFields = selectedFields();
    List<Tuple> selected = new ArrayList<Tuple>(sorted.size());
    for (Tuple tuple : sorted) {
      Tuple filtered = new Tuple(tuple).remove(declaredFields, selectedFields);
      selected.add(filtered);
    }
    return Collections.unmodifiableList(selected);
  }

  /**
   * Returns the result as a {@link TupleEntry} list.
   */
  public List<TupleEntry> asTupleEntryList() {
    List<Tuple> tuples = asTupleList();
    Fields selectedFields = selectedFields();
    List<TupleEntry> tupleEntries = new ArrayList<TupleEntry>(tuples.size());
    for (Tuple tuple : tuples) {
      tupleEntries.add(new TupleEntry(selectedFields, tuple, true));
    }
    return Collections.unmodifiableList(tupleEntries);
  }

  public PrettyPrinter prettyPrinter() {
    return new PrettyPrinter(this);
  }

  /**
   * Returns the set of {@link Fields} selected on this result.
   */
  Fields selectedFields() {
    return declaredFields.select(withFields);
  }

  List<Tuple> getTuples() {
    return tuples;
  }

  Fields getDeclaredFields() {
    return declaredFields;
  }

  @Override
  public int hashCode() {
    return Objects.hash(declaredFields, tuples);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Data other = (Data) obj;
    return Objects.equals(declaredFields, other.declaredFields) && Objects.equals(tuples, other.tuples);
  }

  @Override
  public String toString() {
    return prettyPrinter().toString();
  }

}
