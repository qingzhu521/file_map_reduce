extern crate rand;

use crate::rand::Rng;
use file_map_reduce::util::BUF_SIZE;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::thread;

fn main() -> std::io::Result<()> {
    let base_num = 10_000_000u64;
    let maximum = 5 * base_num;
    let mut tvec = vec![];
    for i in 0..2 {
        let handler = thread::Builder::new()
            .name(i.to_string())
            .spawn(move || -> std::io::Result<()> {
                let mut rng = rand::thread_rng();
                let mut file_path = String::from("synthetic.data");
                file_path.push_str(i.to_string().as_str());
                let out_file = match File::open(&file_path) {
                    Ok(file) => {
                        println!("File Already Have");
                        file
                    }
                    _ => File::create(&file_path)?,
                };

                let mut bw = BufWriter::with_capacity(BUF_SIZE, out_file);
                for i in 0..maximum / 2 {
                    let mut url_example = String::from("url:://");
                    let rand_part: String = rng.gen::<u64>().to_string();
                    url_example.push_str(rand_part.to_string().as_str());
                    url_example.push('\n');
                    let _res = bw.write(url_example.as_bytes());
                    if i % base_num == 0 {
                        println!(
                            "STAGE ------{:?} / {:?}-----",
                            i / base_num,
                            maximum / base_num
                        );
                    }
                }

                Ok(())
            })
            .unwrap();

        tvec.push(handler);
    }

    for h in tvec.drain(..) {
        let _res = h.join();
    }
    Ok(())
}
