To load mmlspark during running pyspark from notebook

* Setup driver
    ```bash
    export SPARK_HOME=/opt/spark
    export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
    export PYSPARK_PYTHON=/home/vanducng/anaconda3/bin/python
    export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH
    export PYSPARK_DRIVER_PYTHON=jupyter
    export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
    ```
* Run below command
    ```shell
    pyspark --packages com.microsoft.ml.spark:mmlspark_2.11:1.0.0-rc1
    ```