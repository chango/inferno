Unit Tests
----------

To run only the unit tests, and exclude the integration tests:

    diana@ubuntu:~/workspace/inferno$ nosetests --exclude=integration

To run both the unit and integration tests:

    diana@ubuntu:~/workspace/inferno$ nosetests

If the tests were successful, you should see something like the following.

    ----------------------------------------------------------------------
    Ran 100 tests in 1.000s

    OK

The integration test suite assumes you have a running disco cluster.

    diana@ubuntu:~$ disco start
    Master ubuntu:8989 started
 
And that your inferno settings (~/.inferno or /etc/inferno/settings.yaml) 
point to that disco master.

    diana@ubuntu:~$ cat ~/.inferno
    server: disco://localhost
