Inferno Overview
================

Inferno Query Language
----------------------

In its simplest form, you can think of Inferno as a query language for large 
amounts of structured text.

This structured text could be a CSV file, or file containing many lines of 
valid JSON, etc.

**people.json**
::

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

**people.csv**
::

    first,last
    Homer,Simpson
    Manjula,Nahasapeemapetilon
    Herbert,Powell
    Ruth,Powell
    Bart,Simpson
    Apu,Nahasapeemapetilon
    Marge,Simpson
    Janey,Powell
    Maggie,Simpson
    Sanjay,Nahasapeemapetilon
    Lisa,Simpson
    Maggie,Términos

**people.db**

If you had this same data in a database, you would just use SQL to query it.

::

    SELECT last_name, COUNT(*) FROM users GROUP BY last_name;

    Nahasapeemapetilon, 3
    Powell, 3
    Simpson, 5
    Términos, 1

**Terminal**

Or if the data was small enough, you might just use command line utilities.

::

    diana@ubuntu:~$ awk -F ',' '{print $2}' people.csv | sort | uniq -c

    3 Nahasapeemapetilon
    3 Powell
    5 Simpson
    1 Términos

**Inferno**

But those methods don't necessarily scale when you're processing terabytes of 
structured text per day.

Here's what a similar query using Inferno would look like:

::

    InfernoRule(
        name='last_names_csv',
        source_tags=['example:chunk:users'],
        parts_preprocess=[count],
        key_parts=['last'],
        value_parts=['count'],
    )

::

    diana@ubuntu:~$ inferno -i names.last_names_csv

    last,count
    Nahasapeemapetilon,3
    Powell,3
    Simpson,5
    Términos,1

Don't worry about the details - we'll cover this rule in depth 
:doc:`here </counting>`. For now, all you need to know is that Inferno will 
yield the same results by starting a Disco map/reduce job to distributing the 
work across the many nodes in your cluster.
