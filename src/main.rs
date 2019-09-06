use file_map_reduce::map::MapFunction;
use file_map_reduce::reduce::ReduceFunction;
use file_map_reduce::top::get_top_k;
use file_map_reduce::util::io_splite;
use std::thread;

fn main() -> std::io::Result<()> {
    //let mut args: Vec<String> = std::env::args().collect();
    //let file = args.pop().unwrap();
    let mut splite_res = io_splite("urlfile.txt", 4).unwrap();
    let mut map_thread_vec = vec![];
    let thread_number = 4;
    let bucket_number = 2;
    for item in splite_res.drain(..) {
        let map_func = MapFunction::new(item, "urlfile.txt", "tmp", bucket_number);
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
    for _i in 0..1 {
        let reduce_func = ReduceFunction::new("tmp", "statistic", 0, bucket_number, thread_number);
        let handler = thread::spawn(move || -> std::io::Result<()> { reduce_func.reduce() });
        reduce_thread_vec.push(handler);
    }

    for reduce_func in reduce_thread_vec {
        let reduce_res = reduce_func.join().unwrap();
        match reduce_res {
            Ok(()) => println!("Reduce Ok"),
            _ => panic!("Reduce fail"),
        }
    }

    let result = get_top_k("statistic", 2, 100);
    println!("{:?}", result);
    Ok(())
}
