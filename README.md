Saury
============

A matrix operations lib extends some contents from [MLlib](http://spark.apache.org/docs/latest/mllib-guide.html), and can be used in [Spark](http://spark.apache.org/) to handle large-scale matrices. The master branch is in version 0.1-SNAPSHOT.  

##Prerequisites
As this project is an extension for Spark, you need to get the Spark installed first. We build and support this project on Spark 1.0.x version.

The following show how to build and use the library.

##Compile Saury
We have offer a default `build.sbt` file, make sure you have installed [sbt](http://www.scala-sbt.org/), and you can just type `sbt assembly` to get a assembly jar.

##Run Saury Test
We have already offer some tests in `edu.nju.pasalab.test` to test the API in the project. Especially, if you want to test two large matrices multiply, use spark-submit method, and type in command
 
	$.bin/spark-submit \
	 --class edu.nju.pasalab.test.TestMatrixMultiply
	 --master <master-url> \
	 --executor-memory <memory> \
	 --driver-class-path saury-assembly-1.0-SNAPSHOT.jar \
	 saury-assembly-1.0-SNAPSHOT.jar \
	 <input file path A> <input file path B> <output file path> <block num>

**Note:** Here we add a configuration `--driver-class-path` , this is because the pre-built Spark-assembly jar doesn't have any files about netlib-java native compontent, which means you cannot load the native linear algebra library（e.g BLAS）, and have to use java to perform the small split-matrix multiply in every worker. We have do some experiments and find this has a significant performance difference, here you can find more info about the [performance comparison](https://github.com/PasaLab/saury/wiki/Performance-comparison-on-matrices-multiply) and [how to load native library](https://github.com/PasaLab/saury/wiki/How-to-load-native-linear-algebra-library).

**Note:**`<input file path A>` is the file path contains the text-file format matrix. We recommand you put it in the hdfs, and in directory `data` we offer two matrix files, in which every row of matrix likes: `7:1,2,5,2.0,3.19,0,...` the `7` before `:` means this is the 8th row of this matrix, and the numbers after `:` splited by `,` means every element in the row.

**Note:** `<block num>` is the split nums of submatries, if you set it as `10`, which means you split every original large matrix into `10*10=100` blocks. The smaller this argument, the biger every worker get submatrix.When doing experiments, we multiply two 20000 by 20000 matrix together, we set it as 10.         

##Martix Operations API in Saury
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
		<td>get submatrix according to row</td>
        <td>sliceByRow(startRow: Long, endRow: Long)</td>
	</tr>
	<tr>
		<td>get submatrix according to column</td>
        <td>sliceByColumn(startCol: Int, endCol: Int)</td>
	</tr>
	<tr>
		<td>get submatrix</td>
        <td>getSubMatrix(startRow: Long, endRow: Long ,startCol: Int, endCol: Int)</td>
	</tr>
	<tr>
		<td>LU decomposition</td>
        <td>luDecompose(mode: String = "auto")</td>
	</tr>
</table>   

##How to use Saury
Here we mainly talks about some basic classes and object in Saury to introuduce how to use it

###IndexRow
We override class `IndexedRow` in MLlib，it is still a `(Long, Vector)` wraper, usage is the same as `IndexedRow` .

###IndexMatrix
We override class `IndexedRowMatrix` in MLlib，from `RDD[IndexedRow]` to `RDD[IndexRow]`， usage is the same as `IndexedRowMatrix` .

###MTUtils
This object can load file-format matrix from hdfs and tachyon, with `loadMatrixFile(sc: SparkContext, path: String, minPatition: Int = 3)` method