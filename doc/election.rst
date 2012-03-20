Example 2 - Campaign Finance
============================

Rule
----

The inferno map/reduce rule (inferno/test/fixture/test_rules/election.py):

.. literalinclude:: ../test/fixture/test_rules/election.py

Input
-----

Make sure `disco <http://discoproject.org/>`_ is running::

    diana@ubuntu:~$ disco start
    Master ubuntu:8989 started

The 2012 presidential campaign finance data (from the `FEC <http://www.fec.gov/disclosurep/PDownload.do>`_)::

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

Output
------

Contributions by Candidate
~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the contributions_by_candidate_name map/reduce job::

    diana@ubuntu:~$ inferno -s localhost -y /path/test_rules/ -i election.presidential_2012.contributions_by_candidate_name
    2012-03-19 INFO  [inferno.lib.job] Processing tags: ['gov:chunk:presidential_campaign_finance']
    2012-03-19 INFO  [inferno.lib.job] Started job presidential_2012@533:87210:81a1b processing 1 blobs
    2012-03-19 INFO  [inferno.lib.job] Done waiting for job presidential_2012@533:87210:81a1b
    2012-03-19 INFO  [inferno.lib.job] Finished job presidential_2012@533:87210:81a1b

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
