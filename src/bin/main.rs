use file_map_reduce::map::MapFunction;
use file_map_reduce::reduce::ReduceFunction;
use file_map_reduce::top::get_top_k;
use file_map_reduce::util::io_splite;
use std::iter::Iterator;
use std::ops::Add;
use std::thread;
use std::time::Instant;

fn main() -> std::io::Result<()> {
    //let mut args: Vec<String> = std::env::args().collect();
    //let file = args.pop().unwrap();
    let file_name = "target\\test.data";
    let current = Instant::now();
    let map_thread_num = 4;
    let reduce_thread_num = 2;
    let bucket_number = 100;

    let mut splite_res = io_splite(file_name, map_thread_num).unwrap();
    let mut map_thread_vec = vec![];

    let map_out_prefix = String::from("tmp");

    for (index, item) in splite_res.drain(..).enumerate() {
        let map_out_index = map_out_prefix.clone().add(index.to_string().as_str());
        let map_func = MapFunction::new(item, file_name, map_out_index, bucket_number);
        let handler = thread::spawn(move || -> std::io::Result<()> { map_func.map() });
        map_thread_vec.push(handler);
    }
    for map_func in map_thread_vec {
        let map_res = map_func.join().unwrap();
        match map_res {
            Ok(()) => println!("map Ok"),
            _ => panic!("map fail"),
        }
    }
    let mut reduce_thread_vec = vec![];

    let interval = bucket_number / reduce_thread_num;
    let mut modular = bucket_number % reduce_thread_num;
    let mut end = 0;
    for _i in 0..reduce_thread_num {
        let start = end;
        end += if modular != 0 {
            modular -= 1;
            interval + 1
        } else {
            interval
        };
        println!("{} {}", start, end);
        let reduce_func = ReduceFunction::new("tmp", "statistic", start, end, map_thread_num);
        let handler = thread::spawn(move || -> std::io::Result<()> { reduce_func.reduce() });
        reduce_thread_vec.push(handler);
    }
    assert_eq!(end, bucket_number);
    for reduce_func in reduce_thread_vec {
        let reduce_res = reduce_func.join().unwrap();
        match reduce_res {
            Ok(()) => println!("Reduce Ok"),
            _ => panic!("Reduce fail"),
        }
    }

    let result = get_top_k("statistic", 2, 100);
    println!("{:?}", result);

    println!("{:?}", current.elapsed());
    Ok(())
}
