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

Daemon Mode
-----------

You can also run Inferno in **daemon mode**. The Inferno daemon will
continuously monitor the blobs in DDFS and launch new map/reduce jobs to
process the incoming blobs as the minimum blobs counts are met.
Here is the Inferno daemon in action. Notice that it skips the first
**automatic rule** because the minimum blob count was not met. The next
automatic rule's blob count was met, so the Inferno daemon processes those
blobs and then persists the results to a data warehouse.

    $ sudo start inferno
    2012-03-27 31664 [inferno.lib.daemon] Starting Inferno...
    ...
    2012-03-27 31694 [inferno.lib.job] Processing tags:['incoming:server01:chunk:task']
    2012-03-27 31694 [inferno.lib.job] Skipping job task_stats_daily: 8 blobs required, have only 0
    ...
    2012-03-27 31739 [inferno.lib.job] Processing tags:['incoming:server01:chunk:user']
    2012-03-27 31739 [inferno.lib.job] Started job user_stats@534:d6c58:d5dcb processing 1209 blobs
    2012-03-27 31739 [inferno.lib.job] Done waiting for job user_stats@534:d6c58:d5dcb
    2012-03-27 31739 [rules.core.database] user_stats@534:d6c58:d5dcb: Saving user_stats_daily data in /tmp/_defaultdESAa7
    2012-03-27 31739 [rules.core.database] user_stats@534:d6c58:d5dcb: Finished processing 240811902 lines in 5 keysets.
    2012-03-27 31739 [inferno.lib.archiver] Archived 1209 blobs to processed:server01:chunk:user_stats:2012-03-27

Read More
-------------
[More about the daemon mode](doc/daemon.rst)

[On Inferno Keysets](doc/keyset.rst)

[Count Last Name Example](doc/counting.rst)

[Campaign Finance Example](doc/election.rst)

[Inferno Settings](doc/settings.rst)

Build Status: [Travis-CI](http://travis-ci.org/chango/inferno) ![Travis-CI](https://secure.travis-ci.org/chango/inferno.png)
