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
package com.hotels.plunger;

import java.io.PrintStream;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Dumps all {@link Tuple Tuples} to a {@link PrintStream} - do not use this in production. Not thread safe.
 */
public class Dump extends SubAssembly {

  private static final long serialVersionUID = 1L;

  public Dump(Pipe pipe) {
    this("", pipe, SystemPrintStreams.SYSOUT);
  }

  public Dump(String prefix, Pipe pipe) {
    this(prefix, pipe, SystemPrintStreams.SYSOUT);
  }

  public Dump(Pipe pipe, PrintStreamSupplier streamSupplier) {
    this("", pipe, streamSupplier);
  }

  public Dump(Pipe pipe, Fields... fieldsOfInterest) {
    this("", pipe, SystemPrintStreams.SYSOUT, fieldsOfInterest);
  }

  public Dump(String prefix, Pipe pipe, Fields... fieldsOfInterest) {
    this(prefix, pipe, SystemPrintStreams.SYSOUT, fieldsOfInterest);
  }

  public Dump(Pipe pipe, PrintStreamSupplier streamSupplier, Fields... fieldsOfInterest) {
    this("", pipe, streamSupplier, fieldsOfInterest);
  }

  public Dump(String prefix, Pipe pipe, PrintStreamSupplier streamSupplier, Fields... fieldsOfInterest) {
    super(pipe);
    Fields mergedFieldsOfInterest;
    if (fieldsOfInterest != null && fieldsOfInterest.length > 0) {
      mergedFieldsOfInterest = Fields.merge(fieldsOfInterest);
    } else {
      mergedFieldsOfInterest = Fields.ALL;
    }
    pipe = new Each(pipe, new DumpFilter(prefix, streamSupplier, mergedFieldsOfInterest));
    setTails(pipe);
  }

  private static class DumpFilter extends BaseOperation<Void> implements Filter<Void> {

    private static final long serialVersionUID = 1L;

    private final PrintStreamSupplier streamSupplier;
    private final String prefix;

    // This is not threadsafe - does this matter for LocalConnector?
    private volatile boolean firstRecord = true;

    private final Fields fieldsOfInterest;

    private DumpFilter(String prefix, PrintStreamSupplier streamSupplier, Fields fieldsOfInterest) {
      this.prefix = prefix;
      this.streamSupplier = streamSupplier;
      this.fieldsOfInterest = fieldsOfInterest;
    }

    @Override
    public boolean isRemove(@SuppressWarnings("rawtypes") FlowProcess flowProcess, FilterCall<Void> filterCall) {
      PrintStream stream = streamSupplier.getPrintStream();
      if (firstRecord) {
        stream.append(prefix);
        for (Comparable<?> value : filterCall.getArguments().getFields().select(fieldsOfInterest)) {
          stream.append(value.toString());
          stream.append('\t');
        }
        stream.append('\n');
        firstRecord = false;
      }
      stream.append(prefix);
      for (String value : filterCall.getArguments().selectEntry(fieldsOfInterest).asIterableOf(String.class)) {
        stream.append(value);
        stream.append('\t');
      }
      stream.append('\n');
      return false;
    }

  }

  /** Delivers a {@link PrintStream} to the {@link Dump} instance to avoid serialization issues. */
  public static interface PrintStreamSupplier {
    PrintStream getPrintStream();
  }

  /** Default {@link PrintStreamSupplier} implementation for standard system streams. */
  public static enum SystemPrintStreams implements PrintStreamSupplier {
    SYSOUT() {

      @Override
      public PrintStream getPrintStream() {
        return System.out;
      }

    },
    SYSERR() {

      @Override
      public PrintStream getPrintStream() {
        return System.err;
      }

    };

  }

}
