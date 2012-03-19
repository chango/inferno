import re

from inferno.lib.rule import chunk_csv_keyset_stream
from inferno.lib.rule import InfernoRule
from inferno.lib.rule import Keyset


def count(parts, params):
    parts['count'] = 1
    yield parts


def alphanumeric(val):
    return re.sub(r'\W+', ' ', val).strip().lower()


def candidate_filter(parts, params):
    active = [
        'P20002721', # Santorum, Rick
        'P60003654', # Gingrich, Newt
        'P80000748', # Paul, Ron
        'P80003338', # Obama, Barack
        'P80003353', # Romney, Mitt
    ]
    if parts['cand_id'] in active:
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
        parts_preprocess=[
            candidate_filter,
            count],
        csv_fields=(
            'cmte_id', 'cand_id', 'cand_nm', 'contbr_nm', 'contbr_city',
            'contbr_st', 'contbr_zip', 'contbr_employer', 'contbr_occupation',
            'contb_receipt_amt', 'contb_receipt_dt', 'receipt_desc',
            'memo_cd', 'memo_text', 'form_tp', 'file_num',
        ),
        csv_dialect='excel',
        keysets={
            'contributions_by_candidate_name':Keyset(
                key_parts=['cand_nm'],
                value_parts=['count', 'contb_receipt_amt'],
                column_mappings={
                    'cand_nm': 'candidate',
                    'contb_receipt_amt': 'amount',
                },
             ),
            'contributions_by_occupation_and_candidate_name':Keyset(
                key_parts=['contbr_occupation', 'cand_nm'],
                value_parts=['count', 'contb_receipt_amt'],
                column_mappings={
                    'cand_nm': 'candidate',
                    'contb_receipt_amt': 'amount',
                    'contbr_occupation': 'occupation',
                },
             )
        }
    )
]
