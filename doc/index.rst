.. Inferno documentation master file, created by
   sphinx-quickstart on Fri Mar  9 03:19:05 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Example - Campaign Finance
==========================

The inferno map/reduce rule (inferno/test/fixture/test_rules/election.py)::

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

Make sure `disco <http://discoproject.org/>`_ is running::

    diana@ubuntu:~$ disco start
    Master ubuntu:8989 started

Here's our input data (from the `FEC <http://www.fec.gov/disclosurep/PDownload.do>`_)::

    diana@ubuntu:~$ head -4 P00000001-ALL.txt
    cmte_id,cand_id,cand_nm,contbr_nm,contbr_city,contbr_st,contbr_zip,contbr_employer,contbr_occupation,contb_receipt_amt,contb_receipt_dt,receipt_desc,memo_cd,memo_text,form_tp,file_num
    C00410118,"P20002978","Bachmann, Michelle","HARVEY, WILLIAM","MOBILE","AL","366010290","RETIRED","RETIRED",250,20-JUN-11,"","","","SA17A",736166
    C00410118,"P20002978","Bachmann, Michelle","HARVEY, WILLIAM","MOBILE","AL","366010290","RETIRED","RETIRED",50,23-JUN-11,"","","","SA17A",736166
    C00410118,"P20002978","Bachmann, Michelle","BLEVINS, DARONDA","PIGGOTT","AR","724548253","NONE","RETIRED",250,01-AUG-11,"","","","SA17A",749073

Toss the input data into `disco's distributed filesystem <http://discoproject.org/doc/howto/ddfs.html>`_ (ddfs)::

    diana@ubuntu:~$ ddfs chunk gov:chunk:presidential_campaign_finance:2012-03-19 ./P00000001-ALL.txt 
    created: disco://localhost/ddfs/vol0/blob/1c/P00000001-ALL_txt-0$533-86a6d-ec842

Verify that the data is in ddfs::

    diana@ubuntu:~$ ddfs xcat gov:chunk:presidential_campaign_finance:2012-03-19 | head -3
    C00410118,"P20002978","Bachmann, Michelle","HARVEY, WILLIAM","MOBILE","AL","366010290","RETIRED","RETIRED",250,20-JUN-11,"","","","SA17A",736166
    C00410118,"P20002978","Bachmann, Michelle","HARVEY, WILLIAM","MOBILE","AL","366010290","RETIRED","RETIRED",50,23-JUN-11,"","","","SA17A",736166
    C00410118,"P20002978","Bachmann, Michelle","BLEVINS, DARONDA","PIGGOTT","AR","724548253","NONE","RETIRED",250,01-AUG-11,"","","","SA17A",749073

Run the contributions_by_candidate_name map/reduce job::

    diana@ubuntu:~$ inferno -s localhost -y /path/test_rules/ -i election.presidential_2012.contributions_by_candidate_name
    2012-03-19 INFO  [inferno.lib.job] Processing tags: ['gov:chunk:presidential_campaign_finance']
    2012-03-19 INFO  [inferno.lib.job] Started job presidential_2012@533:87210:81a1b processing 1 blobs
    2012-03-19 INFO  [inferno.lib.job] Done waiting for job presidential_2012@533:87210:81a1b
    2012-03-19 INFO  [inferno.lib.job] Finished job presidential_2012@533:87210:81a1b

The output::

    cand_nm,count,contb_receipt_amt
    "Bachmann, Michelle",12322,2607916.06
    "Cain, Herman",19924,7010445.99
    "Gingrich, Newt",27740,9271750.98
    "Huntsman, Jon",4143,3200693.48
    "Johnson, Gary Earl",702,413276.89
    "McCotter, Thaddeus G",74,37030.0
    "Obama, Barack",292400,81057578.81
    "Paul, Ron",87697,15435762.37
    "Pawlenty, Timothy",4532,4238858.94
    "Perry, Rick",13341,18644247.91
    "Roemer, Charles E. 'Buddy' III",5364,339033.78
    "Romney, Mitt",58420,55427338.84
    "Santorum, Rick",9382,3351439.54


Run the contributions_by_occupation_and_candidate_name map/reduce job::

    diana@ubuntu:~$ inferno -s localhost -y /path/test_rules/ -i election.presidential_2012.contributions_by_occupation_and_candidate_name > occupations.csv
    2012-03-19 INFO  [inferno.lib.job] Processing tags: ['gov:chunk:presidential_campaign_finance']
    2012-03-19 INFO  [inferno.lib.job] Started job presidential_2012@533:87782:c7c98 processing 1 blobs
    2012-03-19 INFO  [inferno.lib.job] Done waiting for job presidential_2012@533:87782:c7c98
    2012-03-19 INFO  [inferno.lib.job] Finished job presidential_2012@533:87782:c7c98

The output::

    diana@ubuntu:~$ tail -n 20 occupations.csv 
    YOUTH CARE WORKER,"Paul, Ron",7,268.96
    YOUTH CAREER SPECIALIST,"Obama, Barack",3,96.0
    YOUTH DEVELOPMENT,"Obama, Barack",5,450.0
    YOUTH DIRECTOR,"Obama, Barack",5,550.0
    YOUTH MINISTER,"Obama, Barack",3,275.0
    YOUTH MINISTER,"Paul, Ron",6,230.24
    YOUTH MINISTER,"Santorum, Rick",1,250.0
    YOUTH MINISTRY DIRECTOR,"Paul, Ron",2,150.0
    YOUTH OUTREACH DIRECTOR,"Romney, Mitt",1,1000.0
    YOUTH PROGRAMS DIRECTOR,"Obama, Barack",6,130.0
    YOUTH SERVICE COORDINATOR,"Obama, Barack",5,350.0
    YOUTH SERVICES LIBRARIAN,"Obama, Barack",3,290.0
    YOUTH SPECIALIST,"Obama, Barack",4,525.0
    YOUTH WORKER,"Paul, Ron",8,595.12
    ZEN BUDDHIST PRIEST,"Obama, Barack",1,300.0
    ZEPPOS AND ASSOCIATES,"Obama, Barack",1,1000.0
    ZIG ZAG RESTAURANT GROUP,"Paul, Ron",5,950.0
    ZIMMERMANS DAIRY,"Paul, Ron",5,83.71
    ZOMBIE SLAYER,"Paul, Ron",8,1556.0
    ZOOLOGIST,"Obama, Barack",1,100.0

Example - Count Last Names
==========================

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

.. toctree::
   :maxdepth: 2

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

