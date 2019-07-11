/**
 * Copyright (C) 2014-2019 Expedia Inc.
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cascading.flow.FlowProcess;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.pipe.joiner.JoinerClosure;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * For stubbing {@link BufferCall} in {@link Buffer} implementations. Primarily this class stores the context state, but
 * is also used to deliver argument {@link TupleEntry TupleEntries} and collect output from the buffer.
 * <p/>
 * Make sure that you call {@link #nextOperateCall()} to advance to the next set of argument values prior to calling
 * {@link Buffer#operate(cascading.flow.FlowProcess, BufferCall)}.
 */
public final class BufferCallStub<C> extends AbstractOperationCallStub<C> implements BufferCall<C> {

  private final Iterator<Entry<TupleEntry, List<TupleEntry>>> groupsIterator;
  private Iterator<TupleEntry> valuesIterator;
  private TupleEntry currentGroup;
  private boolean retainValues;

  private BufferCallStub(Fields argumentFields, Fields declaredFields, Map<TupleEntry, List<TupleEntry>> map) {
    super(argumentFields, declaredFields);
    groupsIterator = map.entrySet().iterator();
  }

  @Override
  public TupleEntry getGroup() {
    return currentGroup;
  }

  @Override
  public Iterator<TupleEntry> getArgumentsIterator() {
    return valuesIterator;
  }

  @Override
  public void setRetainValues(boolean retainValues) {
    this.retainValues = retainValues;
  }

  @Override
  public boolean isRetainValues() {
    return retainValues;
  }

  @Override
  public JoinerClosure getJoinerClosure() {
    throw new UnsupportedOperationException("TODO: implement BufferJoin functionality.");
  }

  /** Advances to the next group. */
  public BufferCallStub<C> nextOperateCall() {
    Entry<TupleEntry, List<TupleEntry>> next = groupsIterator.next();
    currentGroup = next.getKey();
    valuesIterator = next.getValue().iterator();
    return this;
  }

  /** Processes the groups with the provided {@link Buffer}. */
  public BufferCallStub<C> complete(FlowProcess<?> flowProcess, Buffer<C> buffer) {
    while (groupsIterator.hasNext()) {
      buffer.prepare(flowProcess, this);
      buffer.operate(flowProcess, nextOperateCall());
    }
    buffer.flush(flowProcess, this);
    buffer.cleanup(flowProcess, this);
    return this;
  }

  public static class Builder<C> {
    private final Fields groupFields;
    private final Fields nonGroupFields;
    private TupleEntry currentGroup;
    private List<TupleEntry> currentValues;
    private Fields outputFields;
    private final Map<TupleEntry, List<TupleEntry>> map = new LinkedHashMap<TupleEntry, List<TupleEntry>>();

    Builder(Fields groupFields, Fields nonGroupFields) {
      if (groupFields == null) {
        throw new IllegalArgumentException("groupFields == null");
      }
      if (nonGroupFields == null) {
        throw new IllegalArgumentException("nonGroupFields == null");
      }
      this.groupFields = groupFields;
      this.nonGroupFields = nonGroupFields;
    }

    /** Specify the output fields when they are different to the nonGroupFields. */
    public Builder<C> outputFields(Fields outputFields) {
      if (outputFields == null) {
        throw new IllegalArgumentException("outputFields == null");
      }
      this.outputFields = outputFields;
      return this;
    }

    /** Creates a new group in the stub record sequence. */
    public Builder<C> newGroup(Object... values) {
      values = FieldTypeValidator.validateValues(groupFields, values);
      flush();
      currentGroup = new TupleEntry(groupFields, new Tuple(values));
      currentValues = new ArrayList<TupleEntry>();
      return this;
    }

    /** Creates a new tuple for the current group in the stub record sequence. */
    public Builder<C> addTuple(Object... values) {
      if (currentGroup == null) {
        throw new IllegalStateException("Must set group before adding tuples.");
      }
      values = FieldTypeValidator.validateValues(nonGroupFields, values);

      currentValues.add(new TupleEntry(nonGroupFields, new Tuple(values)));
      return this;
    }

    private void flush() {
      if (currentGroup != null && currentValues.size() > 0) {
        map.put(currentGroup, currentValues);
      }
    }

    /** Builds the stub instance. */
    public BufferCallStub<C> build() {
      Fields fields;
      if (outputFields != null) {
        fields = outputFields;
      } else {
        fields = nonGroupFields;
      }
      flush();
      return new BufferCallStub<C>(nonGroupFields, fields, map);
    }

  }

}
