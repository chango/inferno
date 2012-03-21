Inferno Keysets
===============

	TODO

key_parts
---------

	TODO

value_parts
-----------

	TODO

column_mappings
---------------

	TODO

table
-----

	TODO

parts_preprocess
----------------

Part pre-pocessors are typically used to filter, expand, or edit the input to
the map step. 

The ``parts_preprocess`` functions are called before the ``field_transforms`` 
functions, to ready the data for the ``map_funtion``.

Note that a ``parts_preprocess`` functions always take ``parts`` and 
``params``, and must ``yield`` one, none, or many parts.

Example parts_preprocess:


field_transforms
----------------

Field transforms are typically used to cast data from one type to another, 
or otherwise prepare the input for the map step. 

Field transform happen before the ``map_funtion`` is called, but after 
``parts_preprocess``.

You often see ``field_transforms`` like ``trim_to_255`` when the results of a 
map/reduce job are persisted to a database in a custom ``result_processor``.

Example field_transforms:

::

    def trim_to_255(val):
        if val is not None:
            return val[:254]
        else:
            return None

::

    def alphanumeric(val):
        return re.sub(r'\W+', ' ', val).strip().lower()

::

    def pad_int_to_10(val):
        return '%10d' % int(val)

::

    def to_int(val):
        try:
            return int(val)
        except:
            return 0

.. code-block:: python
   :emphasize-lines: 5-10

    InfernoRule(
        name='a_rule_with_field_transforms',
        source_tags=['some:ddfs:tag'],
        map_input_stream=chunk_json_keyset_stream,
        field_transforms={
            'key1':trim_to_255,
            'key2':alphanumeric,
            'value1':pad_int_to_10,
            'value2':to_int,
        },
        key_parts=['key1', 'key2', 'key3'],
        value_parts=['value2', 'value2', 'value3'],
    ),
