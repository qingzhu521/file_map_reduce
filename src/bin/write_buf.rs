extern crate rand;

use crate::rand::Rng;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::time::Instant;

fn main() -> std::io::Result<()> {
    let mut args = std::env::args();
    let buff_para = args.nth(1).unwrap();
    let len = buff_para.len();
    let base = if buff_para.ends_with('m') {
        1 << 20
    } else if buff_para.ends_with('k') {
        1 << 10
    } else {
        1
    };
    let pre_num: usize = buff_para[0..len - 1].parse().unwrap();
    let buf_size = pre_num * base;

    let mut rng = rand::thread_rng();
    let file_path = String::from("target\\test.data");
    let out_file = File::create(&file_path)?;

    let base_num = 10_000_000u64;
    let maximum = 5 * base_num;
    let instants = Instant::now();
    let mut bw = BufWriter::with_capacity(buf_size, out_file);
    for i in 0..maximum {
        let mut url_example = String::from("url:://");
        let rand_part: String = rng.gen::<u64>().to_string();
        url_example.push_str(rand_part.to_string().as_str());
        url_example.push('\n');
        let _res = bw.write(url_example.as_bytes());
        if i % base_num == 0 && i != 0 {
            println!(
                "STAGE ------{:?} / {:?}-----",
                i / base_num,
                maximum / base_num
            );
        }
    }
    println!(
        "STAGE ------{:?} / {:?}-----",
        maximum / base_num,
        maximum / base_num
    );
    let duration = instants.elapsed();

    println!("Duration {:?}", duration);
    Ok(())
}
