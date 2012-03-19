import re

from inferno.lib.rule import chunk_csv_keyset_stream
from inferno.lib.rule import InfernoRule
from inferno.lib.rule import Keyset


def count(parts, params):
    parts['count'] = 1
    yield parts


def occupation_preprocess(parts, params):
    parts['occupation'] = re.sub(r'\W+', ' ', parts['contbr_occupation'])
    yield parts


RULES = [
    InfernoRule(
        name='presidential_2012',
        source_tags=['gov:chunk:presidential_campaign_finance'],
        map_input_stream=chunk_csv_keyset_stream,
        parts_preprocess=[count, occupation_preprocess],
        partitions=1,
        csv_fields=(
            'cmte_id',
            'cand_id',
            'cand_nm',
            'contbr_nm',
            'contbr_city',
            'contbr_st',
            'contbr_zip',
            'contbr_employer',
            'contbr_occupation',
            'contb_receipt_amt',
            'contb_receipt_dt',
            'receipt_desc',
            'memo_cd',
            'memo_text',
            'form_tp',
            'file_num',
        ),
        csv_dialect='excel',
        keysets={
            'contributions_by_candidate_name':Keyset(
                key_parts=['cand_nm'],
                value_parts=['count', 'contb_receipt_amt'],
             ),
            'contributions_by_occupation_and_candidate_name':Keyset(
                key_parts=['occupation', 'cand_nm'],
                value_parts=['count', 'contb_receipt_amt']
             )
        }
    )
]
