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
import java.util.Iterator;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.FieldsResolverException;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * For stubbing {@link FunctionCall} in {@link Function} implementations. Primarily this class stores the context state,
 * but is also used to deliver {@link TupleEntry TupleEntries} and collect output from the function.
 * <p/>
 * Make sure that you call {@link #nextOperateCall()} to advance to the next set of argument values prior to calling
 * {@link Function#operate(cascading.flow.FlowProcess, FunctionCall)}.
 */
public final class FunctionCallStub<C> extends AbstractOperationCallStub<C> implements FunctionCall<C> {

  private final Iterator<TupleEntry> arguments;

  private TupleEntry currentArguments;

  private FunctionCallStub(Fields argumentFields, Fields declaredFields, Iterable<TupleEntry> arguments) {
    super(argumentFields, declaredFields);
    this.arguments = arguments.iterator();
  }

  @Override
  public TupleEntry getArguments() {
    return currentArguments;
  }

  /** Advances to the next arguments value. */
  public FunctionCallStub<C> nextOperateCall() {
    currentArguments = arguments.next();
    return this;
  }

  /** Processes the groups with the provided {@link Function}. */
  public FunctionCallStub<C> complete(FlowProcess<?> flowProcess, Function<C> function) {
    function.prepare(flowProcess, this);
    while (arguments.hasNext()) {
      function.operate(flowProcess, nextOperateCall());
    }
    function.flush(flowProcess, this);
    function.cleanup(flowProcess, this);
    return this;
  }

  static public class Builder<C> {
    private final Fields fields;
    private final List<TupleEntry> tuples = new ArrayList<TupleEntry>();
    private Fields fieldMask;
    private Fields outputFields;

    Builder(Fields fields) {
      if (fields == null) {
        throw new IllegalArgumentException("fields == null");
      }
      this.fields = fields;
      fieldMask = fields;
    }

    /** Specify the output fields when they are different to the nonGroupFields. */
    public Builder<C> outputFields(Fields outputFields) {
      if (outputFields == null) {
        throw new IllegalArgumentException("outputFields == null");
      }
      this.outputFields = outputFields;
      return this;
    }

    public Builder<C> addTuple(Object... values) {
      if (values == null || values.length == 0) {
        values = new Object[fieldMask.size()];
      } else if (values.length != fieldMask.size()) {
        throw new IllegalStateException("Value array length not suitable for field mask: " + fieldMask);
      }
      TupleEntry newTuple = new TupleEntry(fields, Tuple.size(fields.size()));
      newTuple.setTuple(fieldMask, new Tuple(values));
      tuples.add(newTuple);
      return this;
    }

    public Builder<C> withFields(Fields... fields) {
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

    public FunctionCallStub<C> build() {
      Fields newFields = outputFields != null ? outputFields : fields;
      return new FunctionCallStub<C>(fields, newFields, tuples);
    }

  }

}
