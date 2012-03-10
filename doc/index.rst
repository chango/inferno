.. Inferno documentation master file, created by
   sphinx-quickstart on Fri Mar  9 03:19:05 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Inferno Documentation
=====================

Must write... :)

A Silly Little Example
======================

The inferno map/reduce rule (inferno/test/fixture/test_rules/names.py)::

    from inferno.lib.rule import chunk_json_keyset_stream
    from inferno.lib.rule import InfernoRule
    from inferno.lib.rule import Keyset


    def count(parts, params):
        parts['count'] = 1
        yield parts


    RULES = [
        InfernoRule(
            name='last_names_json',
            source_tags=['example:chunk:users'],
            map_input_stream=chunk_json_keyset_stream,
            parts_preprocess=[count],
            partitions=2,
            key_parts=['last_name'],
            value_parts=['count'],
        )
    ]


Make sure `disco <http://discoproject.org/>`_ is running::

    diana@ubuntu:~$ disco start
    Master ubuntu:8989 started

Here's our input data::

    diana@ubuntu:~$ cat data.txt 
    {"first_name":"Joan", "last_name":"Términos"}
    {"first_name":"Willow", "last_name":"Harvey"}
    {"first_name":"Noam", "last_name":"Clarke"}
    {"first_name":"Joan", "last_name":"Harvey"}
    {"first_name":"Beatty", "last_name":"Clarke"}

Toss the input data into `disco's distributed filesystem <http://discoproject.org/doc/howto/ddfs.html>`_ (ddfs)::

    diana@ubuntu:~$ ddfs chunk example:chunk:users ./data.txt 
    created: disco://localhost/ddfs/vol0/blob/99/data_txt-0$533-406a9-e50

Verify that the data is in ddfs::

    diana@ubuntu:~$ ddfs xcat example:chunk:users
    {"first_name":"Joan", "last_name":"Términos"}
    {"first_name":"Willow", "last_name":"Harvey"}
    {"first_name":"Noam", "last_name":"Clarke"}
    {"first_name":"Joan", "last_name":"Harvey"}
    {"first_name":"Beatty", "last_name":"Clarke"}

Run the last name counting map/reduce job::

    diana@ubuntu:~$ inferno -s localhost -y /home/diana/workspace/inferno/test/fixture/test_rules -i names.last_names_json
    2012-03-09 03:41:08,765,765 INFO  [inferno.lib.job] Processing tags: ['example:chunk:users']
    2012-03-09 03:41:08,806,806 INFO  [inferno.lib.job] Started job last_names_json@533:40914:c355f processing 1 blobs
    2012-03-09 03:41:12,115,115 INFO  [inferno.lib.job] Done waiting for job last_names_json@533:40914:c355f

The output::

    last_name,count
    Clarke,2
    Harvey,2
    Términos,1


.. toctree::
   :maxdepth: 2



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

