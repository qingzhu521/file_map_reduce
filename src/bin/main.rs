use file_map_reduce::map::MapFunction;
use file_map_reduce::reduce::ReduceFunction;
use file_map_reduce::top::get_top_k;
use file_map_reduce::util::io_splite;
use std::iter::Iterator;
use std::ops::Add;
use std::thread;
use std::time::Instant;

fn main() -> std::io::Result<()> {
    let input_file_name = "target\\test.data";
    let map_out_prefix = "tmp";
    let reduce_out_dir = "statistic";

    let map_thread_num = 1;
    let reduce_thread_num = 1;
    let bucket_number = 100;

    let current = Instant::now();
    let mut splite_res = io_splite(input_file_name, map_thread_num).unwrap();
    let mut map_thread_vec = vec![];

    for (index, item) in splite_res.drain(..).enumerate() {
        let map_out_dir = map_out_prefix
            .to_string()
            .clone()
            .add(index.to_string().as_str());
        let map_func = MapFunction::new(item, input_file_name, map_out_dir, bucket_number);
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
        let reduce_func =
            ReduceFunction::new(map_out_prefix, reduce_out_dir, start, end, map_thread_num);
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

    let result = get_top_k(reduce_out_dir, bucket_number, 100);
    println!("{:?}", result);

    println!("{:?}", current.elapsed());
    Ok(())
}
