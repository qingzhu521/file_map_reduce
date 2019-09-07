# File_Based_Map_Reduce

The project is for finding the most 100 urls in 100GB files which constraint in 1G memory.

Usage:
  `cargo run --bin main -- -i <input>  -m <map_out> -r <reduce_out> -f <number of the map threads> -s <number of the reduce threads> -b <number of buckets>`
 
 ## `Parameters`
 >* -i "input" the path of input url file. 
 >* -m "map_out" a prefix of the file that the map operator can store. 
 >* -r "reduce_out" the director of the file that the reduce operator can store. 
 >* -f "number of the map threads" 
 >* -s "number of the reduce threads"
 >* -b "number of buckets".


## How It Works
1. Map - It reads the url file and hash the URL string into a number which is like number % bucket to specify the index of file that the url belongs.
2. Reduce - It reads the bucket file as map giving and store it into a map. Then it sorts each file into a bucket sorted file as output.
3. Topk - It uses output file of reduce procdure as a input. And use heap iteratively give the top-k elements.

## Experiments
This is a table for experiments[https://docs.google.com/spreadsheets/d/1usG3xcs5iF3F0ls63ppfFUILXCpnaqX4CJNTfUTXXnI/edit#gid=0
].
In experiments first we test the best buffer size of reader and writer.
And then we use brown hash and seahash as comparison with the standard library. 
We superisingly discovered these conclusion.
First usually standard library is more faster than the third part library in most of the time. 
And through our experiments we found multi thread do harm to IO-efficient programming.
In future we need to do more experiment on computer with multi disks to prove it will do better in that environment.
