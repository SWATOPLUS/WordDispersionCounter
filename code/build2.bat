javac -d bin2 WordMax.java -cp C:\hadoop-2.9.2\etc\hadoop;C:\hadoop-2.9.2\share\hadoop\common\lib\*;C:\hadoop-2.9.2\share\hadoop\common\*;C:\hadoop-2.9.2\share\hadoop\hdfs;C:\hadoop-2.9.2\share\hadoop\hdfs\lib\*;C:\hadoop-2.9.2\share\hadoop\hdfs\*;C:\hadoop-2.9.2\share\hadoop\yarn;C:\hadoop-2.9.2\share\hadoop\yarn\lib\*;C:\hadoop-2.9.2\share\hadoop\yarn\*;C:\hadoop-2.9.2\share\hadoop\mapreduce\lib\*;C:\hadoop-2.9.2\share\hadoop\mapreduce\*
jar -cvf wordmax.jar -C bin2/ .
