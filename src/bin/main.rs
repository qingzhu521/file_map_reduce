extern crate clap;
extern crate hashbrown;

use clap::{App, Arg};

use file_map_reduce::map::MapFunction;
use file_map_reduce::reduce::ReduceFunction;
use file_map_reduce::top::get_top_k;
use file_map_reduce::util::io_splite;
use std::iter::Iterator;
use std::ops::Add;
use std::thread;
use std::time::Instant;

fn main() -> std::io::Result<()> {
    let matches = App::new("DISK_BASE_MAP_REDUCE")
        .version("0.2")
        .about("Run pattern matching")
        .args(&[
            Arg::with_name("input")
                .short("i")
                .long("input")
                .required(true)
                .help("The Path of URL File")
                .takes_value(true)
                .index(1),
            Arg::with_name("map_output")
                .short("m")
                .long("map")
                .default_value("tmp")
                .help("The prefix of map output")
                .takes_value(true)
                .index(2),
            Arg::with_name("reduce_output")
                .short("r")
                .long("reduce")
                .default_value("statistic")
                .help("The director of reduce output")
                .takes_value(true)
                .index(3),
            Arg::with_name("number_of_map_threads")
                .short("f")
                .long("threadm")
                .default_value("1")
                .help("The number of threads that map occupe")
                .takes_value(true),
            Arg::with_name("number_of_reduce_threads")
                .short("s")
                .long("threadr")
                .default_value("1")
                .help("The number of threads that reduce occupe")
                .takes_value(true),
            Arg::with_name("number_of_buckets")
                .short("b")
                .long("bucket")
                .default_value("117")
                .help("The number of bucket that hash take")
                .takes_value(true),
        ])
        .get_matches();

    let input_file_name = matches.value_of("input").unwrap().to_string();
    let map_out_prefix = matches.value_of("map_output").unwrap().to_string();
    let reduce_out_dir = matches.value_of("reduce_output").unwrap().to_string();

    let map_thread_num: usize = matches
        .value_of("number_of_map_threads")
        .unwrap()
        .parse()
        .unwrap();
    let reduce_thread_num = matches
        .value_of("number_of_reduce_threads")
        .unwrap()
        .parse()
        .unwrap();
    let bucket_number = matches
        .value_of("number_of_buckets")
        .unwrap()
        .parse()
        .unwrap();

    let current = Instant::now();
    let mut splite_res = io_splite(input_file_name.clone(), map_thread_num).unwrap();
    let mut map_thread_vec = vec![];

    for (index, item) in splite_res.drain(..).enumerate() {
        let map_out_dir = map_out_prefix
            .to_string()
            .clone()
            .add(index.to_string().as_str());
        let mut map_func =
            MapFunction::new(item, input_file_name.clone(), map_out_dir, bucket_number);
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
        let reduce_func = ReduceFunction::new(
            map_out_prefix.clone(),
            reduce_out_dir.clone(),
            start,
            end,
            map_thread_num,
        );
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

    let result = get_top_k(reduce_out_dir.as_str(), bucket_number, 100);
    println!("{:?}", result);
    println!("{:?}", current.elapsed());
    Ok(())
}
