Inferno Command Line Interface
==============================

::

    diana@ubuntu:~$ inferno --help
    usage: inferno [-h] [-v] [-s SERVER] [-e SETTINGS_FILE] [-i IMMEDIATE_RULE]
                   [-y RULES_DIRECTORY] [-q] [-f] [-x] [-D] [-d] [-p]
                   [-t SOURCE_TAGS] [-u SOURCE_URLS] [-r RESULT_TAG]
                   [-S DAY_START] [-R DAY_RANGE] [-O DAY_OFFSET] [-P PARAMETERS]
                   [-l PARAMETER_FILE] [--data-file DATA_FILE]
                   [--example_rules EXAMPLE_RULES]

    Inferno: a python map/reduce library powered by disco.

    optional arguments:
      -h, --help            show this help message and exit
      -v, --version         show program's version number and exit
      -s SERVER, --server SERVER
                            master disco server
      -e SETTINGS_FILE, --settings SETTINGS_FILE
                            path to settings file
      -i IMMEDIATE_RULE, --immediate-rule IMMEDIATE_RULE
                            execute <module>.<rule> immediately and exit
      -y RULES_DIRECTORY, --rules-directory RULES_DIRECTORY
                            directory to search for Inferno rules
      -q, --just-query      print out the blobs of the source query and generated
                            SQL, but don't execute the rule (only useful for
                            debugging rules in immediate mode)
      -f, --force           force processing of blobs
      -x, --start-paused    start Inferno without starting any automatic rules
                            (pause mode)
      -D, --disco-debug     debug map/reduce to disco console
      -d, --debug           debug flag for inferno consumers
      -p, --profile         output disco profiling data
      -t SOURCE_TAGS, --source-tags SOURCE_TAGS
                            override the ddfs source tags
      -u SOURCE_URLS, --source-urls SOURCE_URLS
                            override the source urls
      -r RESULT_TAG, --result-tag RESULT_TAG
                            override the ddfs result tag
      -S DAY_START, --day-start DAY_START
                            override the start day for blobs
      -R DAY_RANGE, --day-range DAY_RANGE
                            override the day range for blobs
      -O DAY_OFFSET, --day-offset DAY_OFFSET
                            override the days previous to start day for blobs
      -P PARAMETERS, --parameters PARAMETERS
                            additional rule parameters (in yaml)
      -l PARAMETER_FILE, --parameter-file PARAMETER_FILE
                            additional rule parameters (in a yaml file)
      --data-file DATA_FILE
                            arbitrary data file made available to job
      --example_rules EXAMPLE_RULES
                            create example rules