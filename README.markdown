REA
===

Implementation for the paper "Top-k Entity Augmentation using Consistent Set Covering".
The following instructions allow to reproduce the paper's results.

Creating an index of **Package rea.scoring**: Contains scoring functions for data sources.the DWTC
-------------------------------

Before you can use REA, you need to acquire a copy of the [Dresden Web Table Corpus](https://wwwdb.inf.tu-dresden.de/misc/dwtc/), and create an Lucene index over it.
You can use the [DWTC-Tools](https://github.com/JulianEberius/dwtc-tools) to perform these tasks. Here is the short version:

    # download the DWTC
    mkdir dwtc
    cd dwtc
    for i in $(seq -w 0 500); do wget http://wwwdb.inf.tu-dresden.de/misc/dwtc/data/dwtc-$i.json.gz; done
    cd ..

    # get the DWTC-Tools
    git clone https://github.com/JulianEberius/dwtc-tools.git
    cd dwtc-tools
    mvn package
    mvn install # needed for the REA compilation
    cd ..

    # create the index
    java -cp dwtc-tools/target/dwtc-tools-1.0.0-jar-with-dependencies.jar webreduce.indexing.Indexer -s -r ./dwtc ./dwtc.lidx

The DWTC plus Index will take > 150GB disk space. It is possible and recommended to place the Index on a different machine than the development machine (see Runtime requirements).


Compilation
-----------

Compilation is done via Maven. Before you can start, you need to install two custom libraries into your local maven repository via "mvn install":

1.) DWTC-Tools: Tools for working with the Dresden Web Table corpus https://github.com/JulianEberius/dwtc-tools . Should already be installed if you followed the above instructions for creating an index.
2.) matchtools: Custom schema and instance matching library https://github.com/JulianEberius/dwtc-tools .

    git clone https://github.com/JulianEberius/matchtools.git
    cd matchtools
    mvn install
    cd ..

Then, in the REA source directory, run

    mvn package
    ./deps_only_jar.sh # a jar that bundles all external dependencies, used by the test scripts to run REA

Notice that REA uses Amazons [Alexa Web Information Service](http://aws.amazon.com/de/awis/) to obtain web domain popularity scores as one scoring component.
To use this scoring component, you need to provide your AWS credentials in rea/knowledge/Domains.scala before compiling.


Runtime requirements
------------------

REA uses [Redis](redis.io) for caching, so install and start it before continuing.
It is trivial to build from source, but should also be available in your OS package repositories. On a Mac using [Homebrew](http://brew.sh/) you can just

    brew install redis

While REA can use the class LocalIndex to work on an Lucene index on the local disk, the default procedure at the moment is to run the IndexServer class, which makes the Lucene index available to the rest of the code as an HTTP service. This allows to place the index on a different machine than the REA test/development machine.
To run the index server:

    test-scripts/run.sh rea.server.IndexServer 8765 dwtc.lidx

Then set the URL and port of your IndexServer both in test-scripts/test-allInOne.sh and test-scripts/test-correctnes.sh.


Running tests
-------------

With Redis running, from the REA source directory, run

    bash test-scripts/test-all.sh

Then you can regenerate the CSV files used in the paper using

    bash eval-scripts/regenCSVs.sh


A Tour around the Code
----------------------

The code can be edited using the Scala IDE (for Eclipse) when using the "Maven Integration for Scala IDE" plugin.

Points of interest:

**File REA.scala**: Contains the web table retrieval and matching system that produces the candidates for the set cover algorithms.
**Package rea.cover**: Contains the implementations of the set cover algorithms discussed in the paper.
**Package rea.test**: Executable classes used in the evaluation. The file *AllInOneTestInverseSim.scala* shows how the matching system and the set covering algorithms are used in together.
**Package rea.coherence**: Contains coherence/consistency measures for data sources.
**Package rea.scoring**: Contains scoring functions for data sources.
**Package rea.index**: Contains the code for working with local or remote Lucene indices and retrieving candidate web tables from them.
**Package rea.definitions**: Classes representing basic concepts, such as dataset, value, cover, etc. and lots of utility functions for them.
**Package rea.server**: Contains the IndexServer that makes DWTC Lucene indices available as an HTTP service.

