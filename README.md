#  matrixlib 说明

##文件组织结构

	data - 测试数据样例
    src  - 源代码目录
	├─main
	│  └─scala
	│      └─edu
	│          └─nju
	│              └─yabby
    │                 └─matrixlib     -矩阵类库文件
    │                 └─test          -矩阵操作相关测试文件
    │
	└─build.sbt  -sbt编译所用依赖说明文件
	

## 类库简介

###IndexRow

重写了mllib的类IndexedRow，还是对(Long, Vector)的封装，主要重写了toString函数，以优化保存时的格式

###IndexMatrix

重写了mllib的类IndexedRowMatrix，从RDD[IndexedRow]修改成RDD[IndexRow]，（目前）提供了分布式矩阵的乘法的API

**注意** 更多关于mllib的矩阵相关内容可以参考网页[mllib-basics](http://spark.apache.org/docs/latest/mllib-basics.html "mllib-basics about vectors and matrices" )

## 使用方法

### 初始化IndexMatrix

使用工具类MTUtils的loadMatrixFile(SparkContext, String) 从文本文件中初始化得到分布式矩阵

### 矩阵操作

参照breeze的[Linear Algebra Cheat sheet](https://github.com/scalanlp/breeze/wiki/Linear-Algebra-Cheat-Sheet "Linear Algebra Cheat sheet")将陆续补充相关矩阵的操作API，目前提供：

* 两个矩阵相乘 multiply(B: IndexMatrix, blkNum: Int)
* 两个矩阵相加 add(B: IndexMatrix)
* 两个矩阵相减 minus(B: IndexMatrix)
* 矩阵A与标量的逐个元素相加 elemWiseAdd(b: Double)
* 矩阵A与标量的逐个元素相减 elemWiseMinus(b: Double)
* 矩阵A与标量的逐个元素相乘 elemWiseMult(b: Double)
* 矩阵A与标量的逐个元素相除 elemWiseDivide(b: Double)
* 获取矩阵给定范围行 sliceByRow(startRow: Long, endRow: Long)
* 获取矩阵给定范围列 sliceByColumn(startCol: Int, endCol: Int)
* 获取矩阵给定范围子矩阵 getSubMatrix(startRow: Long, endRow: Long ,startCol: Int, endCol: Int)

**注意** 获取给定范围行和列时，上下界参数都是包含关系


### 矩阵相关操作测试

* 测试矩阵乘法

使用类的TestMatrixMultiply测试两个矩阵的相乘，采用标准spark-submit的操作指令如下：

>$ bin/spark-submit --class edu.nju.yabby.test.TestMatrixMultiply --master <master-url\> \
   <application jar\>  <input file path A\>  <input file path B\>  <output file path\> blockNum 
   
**注意** 参数blockNum表示矩阵每行每列被切分的数量

* 测试矩阵的逐个元素的相关操作

>$ bin/spark-submit --class edu.nju.yabby.test.TestMatrixElemOP --master local <application jar\> 

* 测试矩阵子矩阵获取

>$ bin/spark-submit --class edu.nju.yabby.test.TestMatrixSlice --master local <application jar\> 
