Example 2 - Campaign Finance
============================

In this example we'll introduce a few more key Inferno concepts:

    * Inferno rules with **multiple keysets**
    * **field_transforms**: input data casting
    * **parts_preprocess**: a pre-map hook
    * **parts_postprocess**: a post-reduce hook
    * **column_mappings**: rename columns post-reduce

Inferno Rule
------------

An Inferno rule to query the 2012 presidential campaign finance data. 
(``inferno/example_rules/election.py``):

.. literalinclude:: ../inferno/example_rules/election.py
   :emphasize-lines: 44-49, 58, 61, 66, 69

Input
-----

Make sure `Disco <http://discoproject.org/>`_ is running::

    diana@ubuntu:~$ disco start
    Master ubuntu:8989 started

The 2012 presidential campaign finance 
`data <ftp://ftp.fec.gov/FEC/Presidential_Map/2012/P00000001/P00000001-ALL.zip>`_ 
(from the `FEC <http://www.fec.gov/disclosurep/PDownload.do>`_)::

    diana@ubuntu:~$ head -4 P00000001-ALL.txt
    cmte_id,cand_id,cand_nm,contbr_nm,contbr_city,contbr_st,contbr_zip,contbr_employer,contbr_occupation,contb_receipt_amt...
    C00410118,"P20002978","Bachmann, Michelle","HARVEY, WILLIAM","MOBILE","AL","366010290","RETIRED","RETIRED",250...
    C00410118,"P20002978","Bachmann, Michelle","HARVEY, WILLIAM","MOBILE","AL","366010290","RETIRED","RETIRED",50...
    C00410118,"P20002978","Bachmann, Michelle","BLEVINS, DARONDA","PIGGOTT","AR","724548253","NONE","RETIRED",250...

Place the input data in `Disco's Distributed Filesystem <http://discoproject.org/doc/howto/ddfs.html>`_ (DDFS)::

    diana@ubuntu:~$ ddfs chunk gov:chunk:presidential_campaign_finance:2012-03-19 ./P00000001-ALL.txt 
    created: disco://localhost/ddfs/vol0/blob/1c/P00000001-ALL_txt-0$533-86a6d-ec842

Verify that the data is in DDFS'::

    diana@ubuntu:~$ ddfs xcat gov:chunk:presidential_campaign_finance:2012-03-19 | head -3
    C00410118,"P20002978","Bachmann, Michelle","HARVEY, WILLIAM","MOBILE","AL","366010290","RETIRED","RETIRED",250...
    C00410118,"P20002978","Bachmann, Michelle","HARVEY, WILLIAM","MOBILE","AL","366010290","RETIRED","RETIRED",50...
    C00410118,"P20002978","Bachmann, Michelle","BLEVINS, DARONDA","PIGGOTT","AR","724548253","NONE","RETIRED",250...

Contributions by Candidate
--------------------------

Run the **contributions_by_candidate_name** job::

    diana@ubuntu:~$ inferno -i election.presidential_2012.by_candidate
    2012-03-19 Processing tags: ['gov:chunk:presidential_campaign_finance']
    2012-03-19 Started job presidential_2012@533:87210:81a1b processing 1 blobs
    2012-03-19 Done waiting for job presidential_2012@533:87210:81a1b
    2012-03-19 Finished job presidential_2012@533:87210:81a1b

The output in CSV::

    candidate,count,amount
    gingrich newt,27740,9271750.98
    obama barack,292400,81057578.81
    paul ron,87697,15435762.37
    romney mitt,58420,55427338.84
    santorum rick,9382,3351439.54

The output as a table:

+----------------+---------+-----------------+
| Candidate      | Count   | Amount          |
+================+=========+=================+
| Obama Barack   | 292,400 | $ 81,057,578.81 |
+----------------+---------+-----------------+
| Romney Mitt    |  58,420 | $ 55,427,338.84 |
+----------------+---------+-----------------+
| Paul Ron       |  87,697 | $ 15,435,762.37 |
+----------------+---------+-----------------+
| Gingrich Newt  |  27,740 | $  9,271,750.98 |
+----------------+---------+-----------------+
| Santorum Rick  |   9,382 | $  3,351,439.54 |
+----------------+---------+-----------------+

Contributions by Occupation
---------------------------

Run the **contributions_by_occupation_and_candidate_name** job::

    diana@ubuntu:~$ inferno -i election.presidential_2012.by_occupation > occupations.csv
    2012-03-19 Processing tags: ['gov:chunk:presidential_campaign_finance']
    2012-03-19 Started job presidential_2012@533:87782:c7c98 processing 1 blobs
    2012-03-19 Done waiting for job presidential_2012@533:87782:c7c98
    2012-03-19 Finished job presidential_2012@533:87782:c7c98

The output::

    diana@ubuntu:~$ grep retired occupations.csv
    retired,gingrich newt,8810,2279602.27
    retired,obama barack,74465,15086766.92
    retired,paul ron,9373,1800563.88
    retired,romney mitt,12798,6483596.24
    retired,santorum rick,1752,421952.98

The output as a table:

+------------+---------------+--------+-----------------+
| Occupation | Candidate     | Count  | Amount          |
+============+===============+========+=================+
| Retired    | Obama Barack  | 74,465 | $ 15,086,766.92 |
+------------+---------------+--------+-----------------+
| Retired    | Romney Mitt   | 12,798 | $  6,483,596.24 |
+------------+---------------+--------+-----------------+
| Retired    | Gingrich Newt |  8,810 | $  2,279,602.27 |
+------------+---------------+--------+-----------------+
| Retired    | Paul Ron      |  9,373 | $  1,800,563.88 |
+------------+---------------+--------+-----------------+
| Retired    | Santorum Rick |  1,752 | $    421,952.98 |
+------------+---------------+--------+-----------------+

