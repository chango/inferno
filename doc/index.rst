.. Inferno documentation master file, created by
   sphinx-quickstart on Fri Mar  9 03:19:05 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Inferno
=======

Inferno is a python `map/reduce <http://en.wikipedia.org/wiki/MapReduce>`_ 
library, powered by `Disco <http://discoproject.org/>`_, specializing in the 
following. 

    * Query language for structured text (csv, json)
    * Continuous and scheduled map/reduce daemon
    * Generic distributed computing

Inferno Query Language
----------------------

In its simplest form, you can think of Inferno as a query language for 
**large** amounts of structured text.

That structured text could be a CSV file, or file containing lines of valid 
JSON, etc.

**people.json**
::

    {"first_name":"Joan",   "last_name":"Términos"}
    {"first_name":"Willow",   "last_name":"Harvey"}
    {"first_name":"Noam",     "last_name":"Clarke"}
    {"first_name":"Joan",     "last_name":"Harvey"}
    {"first_name":"Beatty",   "last_name":"Clarke"}

**people.csv**
::

    first_name,last_name
    Joan,Términos
    Willow,Harvey
    Noam,Clarke
    Joan,Harvey
    Beatty,Clarke

**people.db**

If you had this same data in the database, you would use SQL to query it.

::

    SELECT last_name, COUNT(*) FROM users GROUP BY last_name;

    Clarke, 2
    Harvey, 2
    Términos, 1

**Terminal**

Or if the data was small enough, you might just use command line utilities.

::

    diana@ubuntu:~$ awk -F ',' '{print $2}' people.csv | sort | uniq -c

    2 Clarke
    2 Harvey
    1 Términos

**Inferno**

But that's not going to scale if you're processing **10,000,000,000+** of 
these records **per day**.

Here's what a similar query using Inferno would look like:

::

    InfernoRule(
        name='last_names_json',
        source_tags=['test:chunk:users'],
        map_input_stream=chunk_json_keyset_stream,
        parts_preprocess=[count],
        key_parts=['last_name'],
        value_parts=['count'],
    )

::

    diana@ubuntu:~$ inferno -i names.last_names_json

    last_name,count
    Clarke,2
    Harvey,2
    Términos,1

Don't worry about the details - we'll cover this rule in depth 
:doc:`here </counting>`. For now, all you need to know is that Inferno will 
yield the same results by starting a Disco map/reduce job, distributing the 
work across the many nodes in your cluster.


Table of Contents
=================
.. toctree::
     :maxdepth: 1
     
     install
     counting
     election

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

