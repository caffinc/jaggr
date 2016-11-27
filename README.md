# jaggr
Simple JSON Aggregator for Java

## Build Status
![Travis-CI Build Status](https://travis-ci.org/caffinc/jaggr.svg?branch=master)

## Usage
jaggr is on Bintray and Maven Central (Soon):

	<dependency>
	    <groupId>com.caffinc</groupId>
	    <artifactId>jaggr</artifactId>
	    <version>0.2.2</version>
	</dependency>

	<dependency>
	    <groupId>com.caffinc</groupId>
	    <artifactId>jaggr-utils</artifactId>
	    <version>0.2.2</version>
	</dependency>

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