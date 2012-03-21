Inferno Settings
================

Example Rules
-------------

The Inferno project comes with a few example rules. Tell inferno where to
put those rules so that we can test your inferno install.

::

    diana@ubuntu:~$ inferno --example_rules example_rules

    Created example rules dir:

            /home/diana/example_rules

                    __init__.py
                    names.py
                    election.py

Settings File
-------------

On a development machine, the recommended place to put Inferno settings is:
**~/.inferno**

1. Create a file called ``.inferno`` in your home directory.
2. Point it to your Disco master (``server``), and the example rules directory we just created (``rules_directory``).

When you're done, it should look something like the following.

::

    diana@ubuntu:~$ cat /home/diana/.inferno
    server: localhost
    rules_directory: /home/diana/example_rules
