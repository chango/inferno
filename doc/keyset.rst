Inferno Keysets
===============

parts_preprocess
----------------

Part pre-processors are typically used to filter, expand, or modify the data 
before sending it to the map step. 

The **parts_preprocess** functions are called before the **field_transforms** 
to ready the data for the **map_function**.

Note that a ``parts_preprocess`` function always takes ``parts`` and 
``params``, and must ``yield`` one, none, or many parts.

Example parts_preprocess:

::

    def count(parts, params):
        parts['count'] = 1
        yield parts

::

    def geo_filter(parts, params):
        if parts['country_code'] in params.geo_codes:
            yield parts

::

    def insert_country_region(parts, params):
        record = params.geo_ip.record_by_addr(str(parts['ip']))
        parts['country_code'] = record['country_code']
        parts['region'] = record['region']
        yield parts

::

    def slice_phrase(parts, params):
        terms = parts['phrase'].strip().lower().split(' ')
        terms_size = len(terms)
        for index, term in enumerate(terms):
            for inner_index in xrange(index, terms_size):
                slice_val = ' '.join(terms[index:inner_index + 1]).strip()
                parts_copy = parts.copy()
                parts_copy['slice'] = slice_val
                yield parts_copy

.. code-block:: python
   :emphasize-lines: 4-9

    InfernoRule(
        name='some_rule_name',
        source_tags=['some:ddfs:tag'],
        parts_preprocess=[
            insert_country_region,
            geo_filter,
            slice_phrase, 
            count
        ],
        key_parts=['country_code', 'region', 'slice'],
        value_parts=['count'],
    ),

field_transforms
----------------

Field transforms are typically used to cast data from one type to another, 
or otherwise prepare the input for the map step. 

The **field_transforms** happen before the **map_function** is called, but 
after **parts_preprocess** functions.

You often see ``field_transforms`` like ``trim_to_255`` when the results of a 
map/reduce are persisted to a database using a custom ``result_processor``.

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
   :emphasize-lines: 4-9

    InfernoRule(
        name='some_rule_name',
        source_tags=['some:ddfs:tag'],
        field_transforms={
            'key1':trim_to_255,
            'key2':alphanumeric,
            'value1':pad_int_to_10,
            'value2':to_int,
        },
        key_parts=['key1', 'key2', 'key3'],
        value_parts=['value2', 'value2', 'value3'],
    ),

parts_postprocess
-----------------

key_parts
---------

value_parts
-----------

column_mappings
---------------
