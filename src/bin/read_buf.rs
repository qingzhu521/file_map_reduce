use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
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

    let prefix: usize = buff_para[0..len - 1].parse().unwrap();
    let buf_size = prefix * base;

    let file_path = String::from("target\\test.data");
    let input_file = File::open(&file_path)?;

    let instants = Instant::now();
    let br = BufReader::with_capacity(buf_size, input_file);
    let mut count = 0;
    for line in br.lines() {
        if line.unwrap().ends_with('0') {
            count += 1;
        }
    }
    let duration = instants.elapsed();

    println!("{:?} Duration {:?}", count, duration);
    Ok(())
}
