Getting Started
===============

Disco
-----

Before diving into Inferno, you'll need to have a working 
`Disco <http://discoproject.org/>`_ cluster (even if it's just a one node 
cluster on your development machine). 

This only takes a few minutes on a Linux machine, and basically just 
involves installing Erlang and Disco. See 
`Installing Disco <http://discoproject.org/doc/start/install.htm>`_ 
for complete instructions.

Bonus Points: A great talk about 
`Disco and Map/Reduce <http://marakana.com/s/disco_mapreduce,1100/index.html>`_.

Inferno
-------

Inferno is available as a package on the 
`Python Package Index <http://pypi.python.org/pypi/inferno>`_, so you can use 
either `pip <http://www.pip-installer.org>`_ or 
`easy_install <http://packages.python.org/distribute/easy_install.html>`_ 
to install it from there.

::

    diana@ubuntu:~$ sudo easy_install inferno
        -- or --
    diana@ubuntu:~$ sudo pip install inferno

If you've got both Inferno and Disco installed, you should be able to ask 
Inferno for its version number:

::

    diana@ubuntu:~$ inferno --version
    inferno-0.1.17


Source Code
-----------

Both Disco and Inferno are open-source libraries. If you end up writing more 
complicated map/reduce jobs, you'll eventually need to look under the hood. 

 * Inferno: http://bitbucket.org/chango/inferno
 * Disco: https://github.com/discoproject
