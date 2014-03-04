Inferno
=======

Inferno is

1. A **query language** for large amounts of **structured text** (csv, json, etc).
2. A continuous and scheduled **map-reduce daemon** with an HTTP
interface that automatically launches map/reduce jobs to handle a
constant stream of incoming data.

Internally, Inferno uses [Disco](http://discoproject.org/) for launching
map-reduce jobs and operating on big data.

Inferno Query Language
----------------------

In its simplest form, you can think of Inferno as a query language for large
amounts of structured text.  This structured text could be a CSV file, or a
file containing many lines of valid JSON, etc.  For example, consider the
following list of people:

    {"first":"Homer", "last":"Simpson"}
    {"first":"Manjula", "last":"Nahasapeemapetilon"}
    {"first":"Herbert", "last":"Powell"}
    {"first":"Ruth", "last":"Powell"}
    {"first":"Bart", "last":"Simpson"}
    {"first":"Apu", "last":"Nahasapeemapetilon"}
    {"first":"Marge", "last":"Simpson"}
    {"first":"Janey", "last":"Powell"}
    {"first":"Maggie", "last":"Simpson"}
    {"first":"Sanjay", "last":"Nahasapeemapetilon"}
    {"first":"Lisa", "last":"Simpson"}
    {"first":"Maggie", "last":"Términos"}

If you had this same data in a database, you would just use SQL to query it.

    > SELECT last_name, COUNT(*) FROM users GROUP BY last_name;

    Nahasapeemapetilon, 3
    Powell, 3
    Simpson, 5
    Términos, 1

Or if the data was small enough, you might just use command line utilities.

    $ awk -F ',' '{print $2}' people.csv | sort | uniq -c

    3 Nahasapeemapetilon
    3 Powell
    5 Simpson
    1 Términos

However, those methods do not necessarily scale when you are processing
terabytes of data per day.

Here's what a similar query in Inferno looks like.  Assuming that the input data
is in Disco distributed filesystem with the 'example:chunk:users' tag.  We
create the following rule and put it in names.py:

    InfernoRule(
        name='last_names_json',
        source_tags=['example:chunk:users'],
        map_input_stream=chunk_json_keyset_stream,
        parts_preprocess=[count],
        key_parts=['last'],
        value_parts=['count'],
    )

Then we query the data as follows:

    $ inferno -i names.last_names_json

    last,count
    Nahasapeemapetilon,3
    Powell,3
    Simpson,5
    Términos,1
