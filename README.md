# jaggr
Simple JSON Aggregator for Java

## Build Status
![Travis-CI Build Status](https://travis-ci.org/caffinc/jaggr.svg?branch=master)

## Usage

### Adding dependency
jaggr is on Bintray and Maven Central (Soon):

	<dependency>
	    <groupId>com.caffinc</groupId>
	    <artifactId>jaggr</artifactId>
	    <version>0.5.0</version>
	</dependency>

	<dependency>
	    <groupId>com.caffinc</groupId>
	    <artifactId>jaggr-utils</artifactId>
	    <version>0.5.0</version>
	</dependency>

### Aggregating documents
Assume the following JSON documents are stored in a file called `raw.json`:

	{"_id": 1, "f": "a", "test": {"f": 3}}
	{"_id": 2, "f": "a", "test": {"f": 2}}
	{"_id": 3, "f": "a", "test": {"f": 1}}
	{"_id": 4, "f": "a", "test": {"f": 5}}
	{"_id": 5, "f": "a", "test": {"f": -1}}
	{"_id": 6, "f": "b", "test": {"f": 1}}
	{"_id": 7, "f": "b", "test": {"f": 1}}
	{"_id": 8, "f": "b", "test": {"f": 1}}
	{"_id": 9, "f": "b", "test": {"f": 1}}
	{"_id": 10, "f": "b", "test": {"f": 1}}

Read it in using the `JsonFileReader` in the `jaggr-utils` module using:

	List<Map<String, Object>> jsonList = JsonFileReader.readJsonFromFile("raw.json");

Now various aggregations can be defined using the `AggregationBuilder`:
	
	Aggregation aggregation = new AggregationBuilder()
	                .setGroupBy(field)
	                .addOperation("avg", new AverageOperation(avgField))
	                .addOperation("sum", new SumOperation(sumField))
	                .addOperation("min", new MinOperation(minField))
	                .addOperation("max", new MaxOperation(maxField))
	                .addOperation("count", new CountOperation())
	                .getAggregation();

Aggregation can now be performed using the `aggregate()` method:

	List<Map<String, Object>> result = aggregation.aggregate(jsonList);

Aggregation also supports Iterators:

	List<Map<String, Object>> result = aggregation.aggregate(jsonList.iterator());

Aggregation actually works with any `Iterable<Map<String, Object>>` too.

The result of the above aggregation would look as follows:

	{"_id": "a", "avg": 2.0, "sum": 10, "min": -1, "max": 5, "count": 5}
	{"_id": "b", "avg": 1.0, "sum": 5, "min": 1, "max": 1, "count": 5}

### Aggregating other data sources

While aggregating files or Lists of JSON documents might be good for some use cases, not all data fits this paradigm.

There are three utilities in the `jaggr-utils` library which can be used to aggregate other sources of data.

#### Aggregating small JSON files in the file system or resources

The `JsonFileReader` class exposes the `readJsonFromFile` and `readJsonFromResource` methods which can be used to read in all the JSON objects from the file into memory for aggregation.

It is generally not a good idea to read in large files due to obvious reasons.

    List<Map<String, Object>> jsonData = JsonFileReader.readJsonFromFile("afile.json");

    List<Map<String, Object>> jsonData = JsonFileReader.readJsonFromResource("aFileInResources.json");

	List<Map<String, Object>> result = aggregation.aggregate(iterator);

#### Aggregating large JSON files or readers

The `JsonStringIterator` class provides constructors to iterate through a JSON file or a `Reader` object pointing to an underlying JSON String source without loading all the data into memory.

    Iterator<Map<String, Object>> iterator = new JsonStringIterator("afile.json");

	Iterator<Map<String, Object>> iterator = new JsonStringIterator(new BufferedReader(new FileReader("afile.json")));

	List<Map<String, Object>> result = aggregation.aggregate(iterator);

#### Aggregating arbitrary object Iterators

The `JsonIterator` abstract class provides a way to convert an `Iterator` from any type to JSON. This can be used to iterate through data coming from arbitrary databases. For example, `MongoDB` provides `Iterable` interfaces to the data. You could aggregate an entire collection as follows:


    Iterator<Map<String, Object>> iterator = new JsonIterator<DBObject>(mongoCollection.find().iterator()) {
        @Override
        public Map<String, Object> toJson(DBObject element) {
            return element.toMap();
        }
    };

	List<Map<String, Object>> result = aggregation.aggregate(iterator);

#### Aggregating batches of data

Starting with version `0.4.0`, `jaggr` supports aggregation of batches of data in a new class called `BatchAggregation`. The following example shows `BatchAggregation` in action:

Input Data:

	{"_id": 1, "f": "a"}
	{"_id": 2, "f": "a"}
	{"_id": 3, "f": "a"}
	{"_id": 4, "f": "a"}
	{"_id": 5, "f": "a"}
	{"_id": 6, "f": "b"}
	{"_id": 7, "f": "b"}
	{"_id": 8, "f": "b"}
	{"_id": 9, "f": "b"}
	{"_id": 10, "f": "b"}

Aggregation:

    BatchAggregation aggregation = new AggregationBuilder()
                .setGroupBy("f")
                .addOperation("count", new CountOperation())
                .getBatchAggregation();

	aggregation.aggregateBatch(jsonData);
	List<Map<String, Object>> result = aggregation.getFinalResult();

Result:

	[
		{"_id":"b","count":5},
		{"_id":"a","count":5}
	]

The `aggregateBatch()` method can be called several times with more data. It can also be chained. 

	result = aggregation
				.aggregateBatch(batch1)
				.aggregateBatch(batch2)
				.getFinalResult();

However the `getFinalResult()` method must be called just once to get the final result of the aggregation, after which the `BatchAggregation` object is reset. It can then be used to aggregate fresh batches of data.


## Supported Aggregations

`jaggr` provides the following aggregations:

1. Count
2. Sum
3. Minimum
4. Maximum
5. Average
6. Collect as List
7. Collect as Set
8. First Object
9. Last Object
10. Standard Deviation (Population)
11. Top N Objects


## Tests

There are extensive tests for each of the aggregations which can be checked out in the [https://github.com/caffinc/jaggr/blob/master/jaggr/jaggr/src/test](https://github.com/caffinc/jaggr/blob/master/jaggr/jaggr/src/test "jaggr tests") file.

There are tests for the jaggr-utils module in [https://github.com/caffinc/jaggr/blob/master/jaggr/jaggr-utils/src/test](https://github.com/caffinc/jaggr/blob/master/jaggr/jaggr-utils/src/test "jaggr-utils tests")

## Dependencies

These are not absolute, but are current (probably) as of 26th November, 2016. It should be trivial to upgrade or downgrade versions as required.

Both `jaggr` and `jaggr-utils` depend on `junit` for tests:

	<dependencies>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.12</version>
            <scope>test</scope>
        </dependency>
	</dependencies>

`jaggr` does not have any other external dependencies, but has a test dependency on `jaggr-utils`.

`jaggr-utils` has the following dependencies:

	<dependencies>
    	<dependency>
            <groupId>com.google.code.gson</groupId>
            <artifactId>gson</artifactId>
            <version>2.6.2</version>
        </dependency>
	</dependencies>

## Help

If you face any issues trying to get this to work for you, shoot me an email: admin@caffinc.com.

Good luck!