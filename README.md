rex-hive-udfs
==================

Rong's Hive UDFs

# How to use

1. Build jar:

```
mvn package
```

This will produce a jar in `target/`
2. Put the jar to HDFS

```
hdfs dfs -put hiveudf-1.0-SNAPSHOT.jar hdfs://hive-udf-path/
```

3. Create function from the uploaded jar for Hive
```
create function udf.array_max as 'com.hive.udf.ArrayMax' using jar 'hdfs://hive-udf-path/hiveudf-1.0-SNAPSHOT.jar';
```

You will need to specify a database as perfix for the udfs (you can just use default database)

4. Use them just like other UDFs
```
SELECT udf.array_max(array(1, 2, 3));
```

# Description of available UDFs
| Return Type | Name(Signature) | Description |
| --- | --- | --- |
| Double | array_avg(array) | Return the average of an array |
| Double | array_sum(array) | Return the sum of an array |
| Double | array_std(array) | Return standard deviation of the array |
| Int | array_count(array) | Count the number of element in an array |
| Int | array_count_distinct(array) | Return the number of unique element in an array |
| Int | array_find(array, val) | Return the index of the first match of the array to the value, -1 will be returned if no match is found |
| T | array_max(array<T>) | Get the maximum element in the array |
| T | array_min(array<T>) | Get the minimum element in the array |
| map<K, Double> | scale_map(map<K, V>, scalar) | Scale each value in the map |
| map<K, Double> | combine_maps(map<K, V>) | Combine maps where values of the same key are added together |
| array\<T\> | array_null_outlier(array\<T\>, lower, upper) | Set those beyond [lower, upper] as null |
| array\<T\> | array_set(array\<T\>, index, val) | Set array[index] = val, exception will throw if the index is beyond the array boundary |
| array\<T\> | array_shift(array\<T\>, val) | Add the given value to the input array and shift out the last element (like queue) |
| array\<T\> | array_unique(array\<T\>, val) | Return an array of all unique element from the input array |
| NULL/Expcetion | assert(condition, message) | Return NULL if the condition is TRUE, otherwise throw a Hive exception with the given message |
| String | geo_circle(lat, lng, radius, sides) | Generate a regular polygon (WKT format) as an approximate of the sepcified circle. The sides needs be an even umber |
| String | hex_encrypt(val, key) | Hex encryption of the val string (uuid) by the key (need to a hex string) |
| Struct<statistic, pvalue> | t_test(x, y) | T-Test for x and y |
