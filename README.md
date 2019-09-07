# File_Based_Map_Reduce

This project is for the test of PingCap.

Usage:
  cargo run --bin main -- <input>  -m <map_out> -r <reduce_out> -f <the number of map thread> -s <the number of reduce thread> -b <the number of the bucket>
  
  The first parameter means the path of input url file. The second is a prefix of the file that the map operator can store. The thrid is similar and it give the director of the file that the reduce operator can store. The fifth means the number of map thread. The forth is the number of the reduce thread. The last means the number of the bucket.


The whole process can be divided into three part. 
* Map:    It reads the url file and hash the URL string into a number which is like number % bucket to specify the index of file that the url belongs.
* Reduce: It reads the bucket file as map giving and store it into a map. Then it sorts each file into a bucket sorted file as output.
* Topk:   It uses output file of reduce procdure as a input. And use heap iteratively give the top-k elements.


This is a table for experiments[https://docs.google.com/spreadsheets/d/1usG3xcs5iF3F0ls63ppfFUILXCpnaqX4CJNTfUTXXnI/edit#gid=0
].
In experiments first we test the best buffer size of reader and writer.
And then we use brown hash and seahash as comparison with the standard library. 
We superisingly discovered these conclusion.
First usually standard library is more faster than the third part library in most of the time. 
And through our experiments we found multi thread do harm to IO-efficient programming.
We need to do more experiment on computer with multi disks  to prove it will do better in that environment.
