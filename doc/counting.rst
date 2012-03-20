Example 1 - Count Last Names
============================

Rule
----

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
            source_tags=['test:integration:chunk:users'],
            map_input_stream=chunk_json_keyset_stream,
            parts_preprocess=[count],
            partitions=2,
            key_parts=['last_name'],
            value_parts=['count'],
        ),
    ]

Input
-----

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

Output
------

Run the last name counting map/reduce job::

    diana@ubuntu:~$ inferno -s localhost -y /path/test_rules -i names.last_names_json
    2012-03-09 INFO [inferno.lib.job] Processing tags: ['example:chunk:users']
    2012-03-09 INFO [inferno.lib.job] Started job last_names_json@533:40914:c355f processing 1 blobs
    2012-03-09 INFO [inferno.lib.job] Done waiting for job last_names_json@533:40914:c355f
    2012-03-09 INFO [inferno.lib.job] Finished job job last_names_json@533:40914:c355f

The output::

    last_name,count
    Clarke,2
    Harvey,2
    Términos,1
