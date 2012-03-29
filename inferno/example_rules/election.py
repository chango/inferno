import re

from inferno.lib.rule import chunk_csv_keyset_stream
from inferno.lib.rule import InfernoRule
from inferno.lib.rule import Keyset


# an example field_transform
def alphanumeric(val):
    return re.sub(r'\W+', ' ', val).strip().lower()


# an example parts_preprocess that modifies the map input
def count(parts, params):
    parts['count'] = 1
    yield parts


# an example parts_preprocess that filters the map input
def candidate_filter(parts, params):
    active = [
        'P20002721',  # Santorum, Rick
        'P60003654',  # Gingrich, Newt
        'P80000748',  # Paul, Ron
        'P80003338',  # Obama, Barack
        'P80003353',  # Romney, Mitt
    ]
    if parts['cand_id'] in active:
        yield parts


# an example parts_postprocess that filters the reduce output
def occupation_count_filter(parts, params):
    if parts['count_occupation_candidate'] > 1000:
        yield parts


RULES = [
    InfernoRule(
        name='presidential_2012',
        source_tags=['gov:chunk:presidential_campaign_finance'],
        map_input_stream=chunk_csv_keyset_stream,
        partitions=1,
        field_transforms={
            'cand_nm':alphanumeric,
            'contbr_occupation':alphanumeric,
        },
        parts_preprocess=[candidate_filter, count],
        parts_postprocess=[occupation_count_filter],
        csv_fields=(
            'cmte_id', 'cand_id', 'cand_nm', 'contbr_nm', 'contbr_city',
            'contbr_st', 'contbr_zip', 'contbr_employer', 'contbr_occupation',
            'contb_receipt_amt', 'contb_receipt_dt', 'receipt_desc',
            'memo_cd', 'memo_text', 'form_tp', 'file_num',
        ),
        csv_dialect='excel',
        keysets={
            'by_candidate':Keyset(
                key_parts=['cand_nm'],
                value_parts=['count', 'contb_receipt_amt'],
                column_mappings={
                    'cand_nm': 'candidate',
                    'contb_receipt_amt': 'amount',
                },
             ),
            'by_occupation':Keyset(
                key_parts=['contbr_occupation', 'cand_nm'],
                value_parts=['count', 'contb_receipt_amt'],
                column_mappings={
                    'count': 'count_occupation_candidate',
                    'cand_nm': 'candidate',
                    'contb_receipt_amt': 'amount',
                    'contbr_occupation': 'occupation',
                },
             )
        }
    )
]
