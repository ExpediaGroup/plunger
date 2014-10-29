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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;

import cascading.tuple.Fields;
import cascading.tuple.FieldsResolverException;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;

/**
 * Convenience class for building {@link Tuple} based input data for {@link PlungerFlow} based cascading unit tests.
 * 
 * @see {@link Data}, {@link TupleListTap}.
 */
public class DataBuilder {

  private final List<Tuple> list;
  private final Fields fields;
  private final Class<?>[] types;
  private TupleEntry tupleEntry;
  private Fields fieldMask;

  /**
   * Constructs a new tuple source builder that will provide {@link Tuple Tuples} that contain values consistent with
   * the declared {@link Fields}.
   */
  public DataBuilder(Fields fields) {
    this(fields, fields.getTypesClasses());
  }

  /**
   * Constructs a new tuple source builder that will provide {@link Tuple Tuples} that contain values consistent with
   * the declared {@link Fields}. Adding tuples will also coerce types.
   */
  public DataBuilder(Fields fields, Class<?>[] types) {
    this.fields = fields;
    fieldMask = fields;
    this.types = types;
    list = new ArrayList<Tuple>();
    if (types != null && types.length != fields.size()) {
      throw new IllegalArgumentException("There must be the same number of types as fields");
    }
  }

  /** Creates a new {@link Tuple} with {@code null} values for each field. */
  public DataBuilder newTuple() {
    flushTupleEntry();
    tupleEntry = new TupleEntry(fields, new Tuple(new Object[fields.size()]));
    return this;
  }

  /** Creates a new {@link Tuple} with the specified values. */
  public DataBuilder addTuple(Object... values) {
    newTuple();
    setTuple(values);
    return this;
  }

  /** Creates a new {@link Tuple} with the specified values. */
  public DataBuilder addTuple(Tuple tuple) {
    newTuple();
    List<Object> elements = Tuple.elements(tuple);
    setTuple(elements.toArray(new Object[elements.size()]));
    return this;
  }

  /** Creates a new {@link TupleEntry} with the specified values. */
  public DataBuilder addTupleEntry(TupleEntry entry) {
    addTuple(entry.getTuple());
    return this;
  }

  public DataBuilder addTupleAndSet(Map<String, Object> valueMap) {
    newTuple();
    set(valueMap);
    return this;
  }

  /**
   * Defines a subset of fields so that you can modify a smaller, pertinent collection of field values with
   * {@link #addTuple(Object...)}.
   */
  public DataBuilder withFields(Fields... fields) {
    Fields fieldMask = Fields.merge(fields);
    try {
      this.fields.select(fieldMask);
      this.fieldMask = fieldMask;
    } catch (FieldsResolverException e) {
      throw new IllegalArgumentException("selected fields must be contained in record fields: selected fields="
          + fieldMask + ", source fields=" + this.fields);
    }

    return this;
  }

  public DataBuilder withAllFields() {
    fieldMask = fields;
    return this;
  }

  /** Makes a copy of the current {@link Tuple}. */
  public DataBuilder copyTuple() {
    TupleEntry copy = new TupleEntry(tupleEntry);
    flushTupleEntry();
    tupleEntry = copy;
    return this;
  }

  /**
   * Sets the value by field in the current {@link Tuple}. The field specified can be the {@link Fields} instance, the
   * field name, or the field position. Using the {@link FieldTransform} interface you can supply your own transforms to
   * map between {@link Comparable} implementations of your choosing to either a position or name. See also the
   * {@link ServiceLoader} for more information on this.
   */
  public DataBuilder set(Comparable<?> field, Object value) {
    if (tupleEntry == null) {
      throw new IllegalStateException();
    }
    tupleEntry.setRaw(field, value);
    return this;
  }

  /** Sets the value by column name in the current {@link Tuple}. */
  public DataBuilder set(Map<String, Object> valueMap) {
    for (Entry<String, Object> entry : valueMap.entrySet()) {
      set(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public DataBuilder setTuple(Object... values) {
    if (values.length != fieldMask.size()) {
      throw new IllegalArgumentException("arguments.length {" + values.length + "} != current fieldMask.size() {"
          + fieldMask.size() + ", values=" + Arrays.toString(values) + "}");
    }
    for (int i = 0; i < fieldMask.size(); i++) {
      tupleEntry.setRaw(fieldMask.get(i), values[i]);
    }
    return this;
  }

  /** Copies the {@link Tuple Tuples} from the provided {@link Data} into this {@link DataBuilder}. */
  public DataBuilder copyTuplesFrom(Data source) {
    for (Tuple tuple : source.getTuples()) {
      flushTupleEntry();
      tupleEntry = new TupleEntry(fields, new Tuple(tuple));
    }
    return this;
  }

  /**
   * Builds the final {@link Data}.
   */
  public Data build() {
    flushTupleEntry();
    List<Tuple> tuples = Collections.unmodifiableList(new ArrayList<Tuple>(list));
    return new Data(fields, tuples);
  }

  private void flushTupleEntry() {
    if (tupleEntry != null) {
      if (types != null) {
        tupleEntry.setTuple(Tuples.coerce(tupleEntry.selectTuple(fields), types));
      }
      list.add(tupleEntry.getTuple());
    }
  }

}
