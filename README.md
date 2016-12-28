Marlin Analysis Branch
============
A distributed matrix operations library build on top of [Spark 2.0.2](http://spark.apache.org/docs/2.0.2/). 

##Branches Notice
This branch(spark-marlin) is built on a **custom version Spark**(version 2.0.2) to get better performance for matrix operations.

##Build Custom Spark
Currently, this branch is developed on the APIs of Spark 2.0.2 version. The optimized Spark Source code is in the directory 'Spark-2.0.2-src' in this branch. You can download the source code and to build it according to the guidelines [here](http://spark.apache.org/docs/2.0.2/building-spark.html#buildmvn).

**Note:** To use the native linear algebra library e.g BLAS to accelerate the computing, compile Spark source with mvn option `-Pnetlib`.

##Compile Marlin
We have offered a default `pom.xml` file, make sure you have installed `mvn` and you can just type `mvn package` to get a package. 

##Run Marlin
We have already offered some examples in `edu.nju.pasalab.marlin.examples` to show how to use the APIs in the project. For example, if you want to run two large matrices multiplication, use spark-submit method, and type in command
 
	$./bin/spark-submit \
	 --class edu.nju.pasalab.marlin.examples.MatrixMultiply
	 --master <master-url> \
	 --executor-memory <memory> \
	 marlin_2.11-0.3-SNAPSHOT.jar \
	 <matrix A rows length> <matrix A columns length> <matrix B columns length> \
	 <matrix B columns length> <cores cross the cluster>  + optional parameter{<broadcast threshold>}

**Note:** this example use `MTUtils.randomDenVecMatrix` to generate distributed random matrix in-memory without reading data from files.

**Note:** `<cores cross the cluster>` is the num of cores across the cluster you want to use. 

**Note:** `optional parameter{<broadcast threshold>}` is to set the threshold for Broadcast Matrix Multiply.

##Martix Operations API in Marlin
Currently, we have finished some APIs, you can find documentation in this [page](https://github.com/PasaLab/marlin/wiki/Linear-Algebra-Cheat-Sheet).

##Contact
gurongwalker at gmail dot com

myasuka at live dot com
