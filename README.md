# File_Based_Map_Reduce
**File_Based_Map_Reduce** is used to find the most 100 urls in 100GB files with 1GB memory constriant.

 ## Usage
  `cargo run --bin main -- -i <input>  -m <map_out> -r <reduce_out> -f <number of the map threads> -s <number of the reduce threads> -b <number of buckets>`
 
 ##### Parameters
    -i   input - the path of input url file  
    -m   map_out - a prefix of the file written by the map operator 
    -r   reduce_out - the director of the file written by the reduce operator   
    -f   number of the map threads 
    -s   number of the reduce threads  
    -b   number of buckets


## How It Works
1. Map - It reads the file and the URL string will be hashed into a number. `number % bucket` indicates the index of bucket file that the URL belongs.
2. Reduce - It reads the bucket file the map operators give and caculates the number of URLs by inserting to hashmap. The URLs will be sorted and output into ordered bucket files.
3. Topk - It uses ordered bucket files as inputs. Binary Heap will help us realizing top k. 

## Experiments
This is a result [table](https://docs.google.com/spreadsheets/d/1usG3xcs5iF3F0ls63ppfFUILXCpnaqX4CJNTfUTXXnI/edit#gid=0
) for experiments. 

First we tested the best buffer size of reader and writer. 
Then we use [brown hash](https://github.com/rust-lang/hashbrown) and [seahash](https://docs.rs/seahash/2.0.0/seahash/) to compare with the standard library. We superisingly discovered these conclusion:
- Standard library is more faster than the third part library in most of the time. 
- Multi-thread do harm to IO-Efficient programming. 

More experiments are needed with multi-disks system to prove it will have a better performance.
