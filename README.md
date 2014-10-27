Marlin
============

A distributed matrix operations library build on top of [Spark](http://spark.apache.org/). Now, the master branch is in version 0.2-SNAPSHOT.  

##Prerequisites
As Marlin is built on top of Spark, you need to get the Spark installed first.  If you are not clear how to setup Spark, please refer to the guidelines [here](http://spark.apache.org/docs/latest/). Currently, Marlin is developed on the APIs of Spark 1.0.x version.

##Compile Marlin
We have offered a default `build.sbt` file, make sure you have installed [sbt](http://www.scala-sbt.org/), and you can just type `sbt package`	to get a package, or type `sbt assembly` to get a assembly jar. 

**Note:** In `build.sbt` file, the default Spark Version is 1.0.1, and the default Hadoop version is 2.3.0, you can modify the `build.sbt` file to fit your environment.

**Note:** Version of `breeze` in `Spark 1.1.0`  is `0.9` .

##Run Marlin
We have already offered some examples in `edu.nju.pasalab.examples` to show how to use the APIs in the project. For example, if you want to run two large matrices multiplication, use spark-submit method, and type in command
 
	$.bin/spark-submit \
	 --class edu.nju.pasalab.examples.MatrixMultiply
	 --master <master-url> \
	 --executor-memory <memory> \
	 marlin_2.10-0.2-SNAPSHOT.jar \
	 <input file path A> <input file path B> <cores cross the cluster> <output path>

**Notice:** Because the pre-built Spark-assembly jar doesn't have any files about netlib-java native compontent, which means you cannot use the native linear algebra library e.g BLAS to accelerate the computing, but have to use pure java to perform the small block matrix multiply in every worker. We have done some experiments and find it has a significant performance difference between the native BLAS computing and the pure java one, here you can find more info about the [performance comparison](https://github.com/PasaLab/marlin/wiki/Performance-comparison-on-matrices-multiply) and [how to load native library](https://github.com/PasaLab/marlin/wiki/How-to-load-native-linear-algebra-library).

**Note:**`<input file path A>` is the file path contains the text-file format matrix. We recommand you put it in the hdfs, and in directory `data` we offer two matrix files, in which every row of matrix likes: `7:1,2,5,2.0,3.19,0,...` the `7` before `:` means this is the 8th row of this matrix (the row index starts from 0), and the numbers after `:` splited by `,` represent each column element in the row.

**Note:** `<cores cross the cluster>` is the num of cores across the cluster you want to use. 

**Note:** `<output path>` is the file path you want to store the result matrix, this matrix is store in DenseVecMatrix Type 

##Martix Operations API in Marlin
Currently, we have finished below APIs:
<table>
	<tr>
		<td><b>Operation</b></td>
        <td><b>API</b></td>
	</tr>
	<tr>
		<td>Matrix-Matrix addition</td>
        <td>add(B: IndexMatrix)</td>
	</tr>
	<tr>
		<td>Matrix-Matrix minus</td>
        <td>minus(B: IndexMatrix)</td>
	</tr>
	<tr>
		<td>Matrix-Matrix multiplication</td>
        <td>multiply(B: IndexMatrix, blkNum: Int)</td>
	</tr>
	<tr>
		<td>Elementwise addition</td>
        <td>elemWiseAdd(b: Double)</td>
	</tr>
	<tr>
		<td>Elementwise minus</td>
        <td>elemWiseMinus(b: Double) / elemWiseMinusBy(b: Double)</td>
	</tr>
	<tr>
		<td>Elementwise multiplication</td>
        <td>elemWiseMult(b: Double)</td>
	</tr>
	<tr>
		<td>Elementwise division</td>
        <td>elemWiseDivide(b: Double) / elemWiseDivideBy(b: Double) </td>
	</tr>
	<tr>
		<td>Get submatrix according to row</td>
        <td>sliceByRow(startRow: Long, endRow: Long)</td>
	</tr>
	<tr>
		<td>Get submatrix according to column</td>
        <td>sliceByColumn(startCol: Int, endCol: Int)</td>
	</tr>
	<tr>
		<td>Get submatrix</td>
        <td>getSubMatrix(startRow: Long, endRow: Long ,startCol: Int, endCol: Int)</td>
	</tr>
	<tr>
		<td>LU decomposition</td>
        <td>LUDecompose(mode: String = "auto")</td>
	</tr>
</table>   

##Algorithms and Performance Evaluation
###Algorithms
Currently,  we implement the matrix manipulation on Spark with [block matrix parallel algorithms](http://en.wikipedia.org/wiki/Block_matrix#Block_matrix_multiplication) to distribute large scale matrix computation among cluster nodes. The details of the matrix multiplication algorithm is [here](https://github.com/PasaLab/marlin/wiki/Matrix-multiply-algorithm).

###Performance Evaluation
We have done some performance evaluation of Marlin. It can be seen [here](https://github.com/PasaLab/marlin/wiki/Performance-comparison-on-matrices-multiply). We wiil update the wiki page when more results are carried out.

##Contact
gurongwalker at gmail dot com

myasuka at live dot com
