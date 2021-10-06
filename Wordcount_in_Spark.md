# Wordcount in Spark

### Setup

Let's setup Spark Colab environment.  Run the cell below!


```python
!pip install pyspark
!pip install -U -q PyDrive
!apt install openjdk-8-jdk-headless -qq
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
```

    Collecting pyspark
      Downloading pyspark-3.1.2.tar.gz (212.4 MB)
    [K     |████████████████████████████████| 212.4 MB 67 kB/s 
    [?25hCollecting py4j==0.10.9
      Downloading py4j-0.10.9-py2.py3-none-any.whl (198 kB)
    [K     |████████████████████████████████| 198 kB 35.4 MB/s 
    [?25hBuilding wheels for collected packages: pyspark
      Building wheel for pyspark (setup.py) ... [?25l[?25hdone
      Created wheel for pyspark: filename=pyspark-3.1.2-py2.py3-none-any.whl size=212880768 sha256=31eded5de79abe2117498cb549186935db4396bfbeca160249f369b05f7fd2bf
      Stored in directory: /root/.cache/pip/wheels/a5/0a/c1/9561f6fecb759579a7d863dcd846daaa95f598744e71b02c77
    Successfully built pyspark
    Installing collected packages: py4j, pyspark
    Successfully installed py4j-0.10.9 pyspark-3.1.2
    The following additional packages will be installed:
      openjdk-8-jre-headless
    Suggested packages:
      openjdk-8-demo openjdk-8-source libnss-mdns fonts-dejavu-extra
      fonts-ipafont-gothic fonts-ipafont-mincho fonts-wqy-microhei
      fonts-wqy-zenhei fonts-indic
    The following NEW packages will be installed:
      openjdk-8-jdk-headless openjdk-8-jre-headless
    0 upgraded, 2 newly installed, 0 to remove and 37 not upgraded.
    Need to get 36.5 MB of archives.
    After this operation, 143 MB of additional disk space will be used.
    Selecting previously unselected package openjdk-8-jre-headless:amd64.
    (Reading database ... 155047 files and directories currently installed.)
    Preparing to unpack .../openjdk-8-jre-headless_8u292-b10-0ubuntu1~18.04_amd64.deb ...
    Unpacking openjdk-8-jre-headless:amd64 (8u292-b10-0ubuntu1~18.04) ...
    Selecting previously unselected package openjdk-8-jdk-headless:amd64.
    Preparing to unpack .../openjdk-8-jdk-headless_8u292-b10-0ubuntu1~18.04_amd64.deb ...
    Unpacking openjdk-8-jdk-headless:amd64 (8u292-b10-0ubuntu1~18.04) ...
    Setting up openjdk-8-jre-headless:amd64 (8u292-b10-0ubuntu1~18.04) ...
    update-alternatives: using /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/orbd to provide /usr/bin/orbd (orbd) in auto mode
    update-alternatives: using /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/servertool to provide /usr/bin/servertool (servertool) in auto mode
    update-alternatives: using /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/tnameserv to provide /usr/bin/tnameserv (tnameserv) in auto mode
    Setting up openjdk-8-jdk-headless:amd64 (8u292-b10-0ubuntu1~18.04) ...
    update-alternatives: using /usr/lib/jvm/java-8-openjdk-amd64/bin/idlj to provide /usr/bin/idlj (idlj) in auto mode
    update-alternatives: using /usr/lib/jvm/java-8-openjdk-amd64/bin/wsimport to provide /usr/bin/wsimport (wsimport) in auto mode
    update-alternatives: using /usr/lib/jvm/java-8-openjdk-amd64/bin/jsadebugd to provide /usr/bin/jsadebugd (jsadebugd) in auto mode
    update-alternatives: using /usr/lib/jvm/java-8-openjdk-amd64/bin/native2ascii to provide /usr/bin/native2ascii (native2ascii) in auto mode
    update-alternatives: using /usr/lib/jvm/java-8-openjdk-amd64/bin/javah to provide /usr/bin/javah (javah) in auto mode
    update-alternatives: using /usr/lib/jvm/java-8-openjdk-amd64/bin/hsdb to provide /usr/bin/hsdb (hsdb) in auto mode
    update-alternatives: using /usr/lib/jvm/java-8-openjdk-amd64/bin/clhsdb to provide /usr/bin/clhsdb (clhsdb) in auto mode
    update-alternatives: using /usr/lib/jvm/java-8-openjdk-amd64/bin/extcheck to provide /usr/bin/extcheck (extcheck) in auto mode
    update-alternatives: using /usr/lib/jvm/java-8-openjdk-amd64/bin/schemagen to provide /usr/bin/schemagen (schemagen) in auto mode
    update-alternatives: using /usr/lib/jvm/java-8-openjdk-amd64/bin/xjc to provide /usr/bin/xjc (xjc) in auto mode
    update-alternatives: using /usr/lib/jvm/java-8-openjdk-amd64/bin/jhat to provide /usr/bin/jhat (jhat) in auto mode
    update-alternatives: using /usr/lib/jvm/java-8-openjdk-amd64/bin/wsgen to provide /usr/bin/wsgen (wsgen) in auto mode


Lets authenticate a Google Drive client to download the file we will be processing in on Spark job.


```python
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.colab import auth
from oauth2client.client import GoogleCredentials

# Authenticate and create the PyDrive client
auth.authenticate_user()
gauth = GoogleAuth()
gauth.credentials = GoogleCredentials.get_application_default()
drive = GoogleDrive(gauth)
```


```python
id='1SE6k_0YukzGd5wK-E4i6mG83nydlfvSa'
downloaded = drive.CreateFile({'id': id})
downloaded.GetContentFile('pg100.txt')
```

By executed the cells above, we can able to see the file *pg100.txt* under the "Files" tab on the left panel.

### Wordcount

After running the setup stage successfully, I am ready to work on the *pg100.txt* file which contains a copy of the complete works of Shakespeare.

I am going to write a Spark application which outputs the number of words that start with each letter. This means that for every letter I want to count the total number of (non-unique) words that start with a specific letter. In my implementation, I am **ignoring the letter case**, i.e., consider all words as lower case. Also, I am ignoring all the words **starting** with a non-alphabetic character.


```python
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext
import pandas as pd

# create the Spark Session
spark = SparkSession.builder.getOrCreate()

# create the Spark Context
sc = spark.sparkContext
```


```python
!head -5 pg100.txt
```

    ﻿The Project Gutenberg EBook of The Complete Works of William Shakespeare, by
    William Shakespeare
    
    This eBook is for the use of anyone anywhere at no cost and with
    almost no restrictions whatsoever.  You may copy it, give it away or



```python
pg100 = sc.textFile('pg100.txt')
```


```python
counts = pg100.flatMap(lambda line: line.split(" ")) \
              .filter(lambda word: len(word) > 0)  \
              .filter(lambda word: ord(word.lower()[0]) in range(ord('a'), ord('z')+1)) \
              .map(lambda word: (word.lower()[0], 1)) \
              .reduceByKey(lambda a, b: a + b)
```


```python
counts.collect()
```




    [('p', 27759),
     ('g', 20782),
     ('c', 34567),
     ('s', 65705),
     ('b', 45455),
     ('i', 62167),
     ('r', 14265),
     ('y', 25855),
     ('l', 29569),
     ('d', 29713),
     ('j', 3339),
     ('h', 60563),
     ('t', 123602),
     ('e', 18697),
     ('o', 43494),
     ('w', 59597),
     ('f', 36814),
     ('u', 9170),
     ('a', 84836),
     ('n', 26759),
     ('m', 55676),
     ('v', 5728),
     ('k', 9418),
     ('q', 2377),
     ('z', 71),
     ('x', 14)]




```python
counts.saveAsTextFile("char_count.txt")
```


```python
sc.stop()
```
