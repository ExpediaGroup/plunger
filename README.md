                  .-..   ____  __    _  _  __ _   ___  ____  ____    ..-.
    .------------|  ||  (  _ \(  )  / )( \(  ( \ / __)(  __)(  _ \   ||  |------------.
    '------------|  ||   ) __// (_/\) \/ (/    /( (_ \ ) _)  )   /   ||  |------------'
                  '-''  (__)  \____/\____/\_)__) \___/(____)(__\_)   ''-'

Use a ``Plunger`` to push test data through your Cascading pipework. Catch the output in a ``Bucket`` and check it for correctness.

## Status ⚠️

This project is no longer in active development.

# Start using
You can obtain **plunger** from Maven Central :

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.hotels/plunger/badge.svg?subject=com.hotels:plunger)](https://maven-badges.herokuapp.com/maven-central/com.hotels/plunger) ![GitHub license](https://img.shields.io/github/license/ExpediaGroup/plunger.svg)


* For Cascading 3.x.x use plunger-3.0.2.
* For Cascading 2.x.x use plunger-2.1.1.


# Overview
**plunger** is a unit testing framework for the [Cascading platform](http://cascading.org "Cascading Application Platform homepage"). It helps you write small and fast unit tests for your ``Assemblies`` with fined grained assertions. A fluent API allows you to rapidly construct test data for delivery via ``Data`` instances and then make assertions on the data captured by ``Buckets``. All test data is maintained in memory to avoid unnecessary I/O overhead. Utility methods are provided as a bridge between input/output files and their in-memory equivalents so that the same style of testing can be applied to ``Flows`` and ``Cascades``.
# Testing Assemblies
#### An end-to-end example
    Plunger plunger = new Plunger();

    Data corpus = new DataBuilder(new Fields("line"))
        .addTuple("one word the found other")
        .addTuple("other waterfalls found of")
        .build();
    Pipe words = plunger.newNamedPipe("words", corpus);

    Pipe assemblyToTest = new WordCountAssembly(words);

    Bucket bucket = plunger.newBucket(new Fields("word", "count"), assemblyToTest);

    List<Tuple> actual = bucket.result().asTupleList();
    assertThat(actual.size(), is(7));
    assertThat(actual.get(0), is(new Tuple("found", 2)));
The above example exhibits many of the core features provided by **plunger**. What follows is a cookbook of the typical usage scenarios:
#### Build test data incrementally
    Data employees = new DataBuilder(new Fields("first", "last", "age"))
        .addTuple("bob", "smith", 28)
        .copyTuple().set("last", "jones")
        .build();
    // bob, smith, 28
    // bob, jones, 28
#### Only provide the data you need for the test
    Data big = new DataBuilder(manyManyFields)
    	.withFields(new Fields("id", "name", "modified_date"))
    	.addTuple(1, "bill", "2013-01-01")
        .addTuple(2, "dave", "2001-01-02")
    	.build();
#### Apply your test data to your assembly and capture the output
    Pipe inputPipe = plunger.newPipe(inputData);
    Pipe assembly = new SmithFilterAssembly(inputPipe);
    Bucket bucket = plunger.newBucket(outputFields, assembly);
#### Make assertions on the captured data
    List<TupleEntry> tupleEntries = bucket.result().asTupleEntryList();
    assertThat(tupleEntries.size(), is(1));
    assertThat(tupleEntries.get(0).getString("first"), is("Bob"));
Note that **plunger** requires that you connect all of your ``Pipes`` and create all of your ``Buckets`` before attempting to obtain the result from any of your ``Buckets``.
#### Create a sorted view of captured data
To increase the predictability of the results you can optionally pass in one or more sort fields to order the tuples:

    List<Tuple> tuples = bucket.result().orderBy(new Fields("age")).asTupleList();
    Tuple tupleWithLowestAge = tuples.get(0);
# Testing Flows and Cascades
It's nice to be able to perform integration tests on your entire ``Flow`` or ``Cascade``. In these circumstances it's normal for both the input and output to the flows to be in the form of real files. Constructing and maintaining sets of input and expect output files is an arduous task, additionally it is non-trivial to perform anything other than coarse grained assertions on file based output. To assist with the development of such tests **plunger** allows you to sink data to a file, allowing you to construct your test data with the fluent API while supplying it to the flows under test in a file based form. Conversely, ``Data`` instances can be constructed from a ``Tap``, allowing you to read the output files of your flow into memory, and then to make fine grained assertions on them in exactly the same manner as you would when testing an ``Assembly``. This approach is especially useful when you require your test data be stored with a scheme that is cannot be easily created by hand - binary or compressed formats for example. At this time **plunger** can perform direct writes and reads using both local and Hadoop taps, including ``PartitionTaps`` and 'multi-tap' variants.

#### Create test data and sink to a file
    Tap sink = new FileTap(new TextDelimited(fields), "new_test_data_file.tsv")

    Data employees = new DataBuilder(fields)
        .addTuple(1, "bill", "2013-01-01")
        .addTuple(2, "dave", "2001-01-02")
        .build();

    Plunger.writeData(employees).toTap(sink);
    // Creates the file 'new_test_data_file.csv' and appends two records
#### Load flow generated output files into memory
    Tap generated = new Hfs(new TextDelimited(outputFields), "output"));

    // Loads the contents of 'output'    
    List<Tuple> actual = Plunger.readDataFromTap(generated).asTupleList();

    assertThat(actual.size(), is(7));
# Other data related features
#### Pretty printing data
During the development of your tests it can be useful to see what data is being both delivered and collected. The ``Data`` class provides a pretty-print method to deliver a tabular view of their data for quick inspection by you - the developer. To increase readability of the output you can optionally pass in one or more sort fields using the ``orderBy`` method to order the results. The sort uses the natural ordering of the types of each field.  You can also supply a column filter using the ``withFields`` method. Considering our earlier example:

    Bucket bucket = plunger.newBucket(new Fields("word", "count"), assembly);
    bucket.result().prettyPrinter().print();

The code above will output the following to ``System.out`` (you can supply another ``PrintStream`` with the ``printTo`` method if you wish):

    word    count
    found   2
    other   2
    the     1
    ...
# Testing Aggregators, Buffers, and Functions
For the most part, it's fairly straight forward to test ``Filter``, ``Function``, ``Aggregator``, and ``Buffer`` classes using only your [favourite mocking](https://code.google.com/p/mockito/) framework. However, it is often the case with aggregator implementations - and to a lesser extent functions - that we need to hold some state between invocations in the operation's ``Context``. It is not always possible to implement this behaviour with our mocks, and even when it is, the resulting code can be rather verbose. **plunger** provides some stub classes to facilitate the testing of aggregators and functions that use the ``OperationCall.Context``. Additionally they allow sets of test data to be fluently declared and operation output captured for later validation in much the same way as the ``DataBuilder`` and ``Bucket`` classes. Here is an aggregator example.

    Aggregator<Context> aggregator = new MyLast(FIELDS);

    AggregatorCallStub<Context> stubCall = Plunger.<Context>newAggregatorCallStubBuilder(GROUP_FIELDS, FIELDS)
        .newGroup(1)
        .addTuple("2013-01-01")
        .addTuple("2013-01-02")
        .build()
        .complete(mockFlowProcess, aggregator);

    List<TupleEntry> collected = stubCall.result().asTupleEntryList();
    assertThat(collected.size(), is(1));
    assertThat(collected.get(0), is(new TupleEntry(FIELDS, new Tuple("2013-01-02"));

Note that all tuples added using ``addTuple`` are associated with the group declared by the most recent ``newGroup`` call. The ``FunctionCallStub`` and ``BufferCallStub`` classes operate in a very similar manner.
# Assertions
#### Verifying serialization
When running Cascading jobs on Hadoop it is often a requirement that your Cascading classes and their dependencies  are ``Serializable``. However, this is not necessary when running test jobs in local mode. Consequently serialization is often overlooked during development and problems arise only when first deploying the to a Hadoop environment. To help identify these issues early on in the development process **plunger** provides a convenient assertion which you can use to check your Assemblies, Functions, Filters, and so on:

    import static com.hotels.plunger.asserts.PlungerAssert.serializable;
    ...
    Pipe assembly = new WordCountAssembly(wordsPipe);
    assertThat(assembly, is(serializable())); // Fails if WordCountAssembly cannot be serialized
The ``PlungerAssert`` class also provides a traditional assertion method if that is more your style: ``assertSerializable(Object)``.
#### TupleEntry matching
When verifying the results of your assemblies it can be time consuming to interrogate a ``TupleEntry`` for all its expected values and write assertions for each individually. The resulting code can also be rather verbose. **plunger** provides a ``Matcher`` with convenient overloads to enable simpler assertions of your output:

    import static com.hotels.plunger.asserts.PlungerAssert.tupleEntry;
    ...
    TupleEntry result = ...
    Fields fields = new Fields("word", "count");
    assertThat(result, is(tupleEntry(new TupleEntry(fields, new Tuple("found", 2)))));
    assertThat(result, is(tupleEntry(fields, new Tuple("found", 2))));
    assertThat(result, is(tupleEntry(fields, "found", 2)));

# Debug output
For the most part Cascading flows can be debugged with your favourite IDE's debugger and Cascading's `LocalFlowConnector`. However, sometimes it's useful to quickly see what fields and values are flowing through your pipes. Cascading provides the `cascading.operation.Debug` operation for printing the data in your pipes out to either STDOUT or STDERR. **plunger** tries to simplify this a step further with the `Dump` assembly:

    pipe = new Dump(pipe); // To STDOUT with no prefix
    pipe = new Dump("prefix:\t", pipe, SYSERR) // prefix all output, use a PrintStream of our choosing

# Building
This project uses the [Maven](http://maven.apache.org/) build system. It also naturally has dependencies on some Cascading artifacts which can be found in the [ConJars](http://conjars.org/) repository. To use this repository you may need to add the following stanza to your Maven repository configuration:

    <repository>
      <id>conjars.org</id>
      <url>http://conjars.org/repo</url>
    </repository>

# Dependencies
Plunger expects the following dependencies to be provided:

* Cascading SDK ≥ 2.6.1
* Hadoop ≥ 2.4.0.2.1.3.0-563
* JUnit ≥ 4.11
* Hamcrest core ≥ 1.3

Earlier versions may work but have not been tested.

# Credits

Created by [Elliot West](https://github.com/teabot), with thanks to: [Dave Maughan](https://github.com/nahguam), [Patrick Duin](https://github.com/patduin), [James Grant](https://github.com/noddy76), [Adrian Woodhead](https://github.com/massdosage), Sven Zethelius.

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2014-2019 Expedia, Inc.
