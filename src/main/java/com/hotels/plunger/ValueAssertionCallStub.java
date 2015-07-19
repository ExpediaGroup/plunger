package com.hotels.plunger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.ValueAssertion;
import cascading.operation.ValueAssertionCall;
import cascading.tuple.Fields;
import cascading.tuple.FieldsResolverException;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * For stubbing {@link ValueAssertionCall} in {@link ValueAssertion} implementations. Primarily this class stores the
 * context state, but is also used to deliver {@link TupleEntry TupleEntries} and collect output from the function.
 * <p/>
 * Make sure that you call {@link #nextOperateCall()} to advance to the next set of argument values}.
 */
public final class ValueAssertionCallStub<C> extends AbstractOperationCallStub<C> implements ValueAssertionCall<C> {

  private final Iterator<TupleEntry> arguments;

  private TupleEntry currentArguments;

  private ValueAssertionCallStub(Fields argumentFields, Fields declaredFields, Iterable<TupleEntry> arguments) {
    super(argumentFields, declaredFields);
    this.arguments = arguments.iterator();
  }

  @Override
  public TupleEntry getArguments() {
    return currentArguments;
  }

  /** Advances to the next arguments value. */
  public ValueAssertionCallStub<C> nextAssertionCall() {
    currentArguments = arguments.next();
    return this;
  }

  /** Processes the groups with the provided {@link Function}. */
  public ValueAssertionCallStub<C> complete(FlowProcess<?> flowProcess, ValueAssertion<C> valueAssertionCall) {

    valueAssertionCall.prepare(flowProcess, this);

    while (arguments.hasNext()) {
      valueAssertionCall.doAssert(flowProcess, nextAssertionCall());
    }
    valueAssertionCall.flush(flowProcess, this);
    valueAssertionCall.cleanup(flowProcess, this);
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

    public Builder<C> addTuple(Object... values) {
      values = FieldTypeValidator.validateValues(fieldMask, values);
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

    public ValueAssertionCallStub<C> build() {
      Fields newFields = outputFields != null ? outputFields : fields;
      return new ValueAssertionCallStub<C>(fields, newFields, tuples);
    }
  }
}
