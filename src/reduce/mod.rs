use std::collections::HashMap;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::{Path, PathBuf};

///
/// The File Reduce Function
///
/// Reduce procedure read url from bucket file that Map just create.
/// It will use hashmap to give a count to every url and store it
/// to a result file.
///
/// *[`Clone`]
///
#[derive(Clone)]
pub struct ReduceFunction<P: AsRef<Path>> {
    input_prefix: P,
    output_prefix: P,
    bucket_start: usize,
    bucket_end: usize,
    pre_thread_num: usize,
}

impl<P: AsRef<Path>> ReduceFunction<P> {
    pub fn new(
        input_prefix: P,
        output_prefix: P,
        bucket_start: usize,
        bucket_end: usize,
        pre_thread_num: usize,
    ) -> Self {
        Self {
            input_prefix,
            output_prefix,
            bucket_start,
            bucket_end,
            pre_thread_num,
        }
    }

    pub fn reduce(&self) -> std::io::Result<()> {
        for i in self.bucket_start..self.bucket_end {
            let mut hmap = HashMap::<String, u64>::new();

            for j in 0..self.pre_thread_num {
                let mut strbuf = String::new();
                strbuf.push_str(self.input_prefix.as_ref().to_str().unwrap());
                strbuf.push_str(j.to_string().as_str());
                println!("{:?}", strbuf);
                let prefixbuf = PathBuf::from(strbuf);

                let prefix = prefixbuf.as_path().join(i.to_string().as_str());
                let f = match File::open(prefix) {
                    Ok(file) => file,
                    _ => panic!("Error Open Reduce File"),
                };

                let bucket_reader = BufReader::with_capacity(1 << 20, f);

                for line in bucket_reader.lines() {
                    let url = line.unwrap();
                    *hmap.entry(url).or_default() += 1;;
                }
            }
            let mut url_vec = vec![];
            for (url, num) in hmap.drain() {
                url_vec.push((num, url));
            }
            url_vec.sort_unstable_by(|a, b| a.0.cmp(&b.0).reverse());

            let path_buf = self.output_prefix.as_ref().join(i.to_string().as_str());
            let outpath = path_buf.as_path();
            println!("{:?}", outpath);

            match std::fs::create_dir(self.output_prefix.as_ref()) {
                Ok(()) => println!("Create OK"),
                _ => println!("Already have"),
            };
            let out_file = File::create(outpath)?;

            let mut bffwriter = BufWriter::with_capacity(1 << 20, out_file);
            for (num, url) in url_vec.drain(..) {
                bffwriter.write_fmt(format_args!("{} {:?}\n", num, url))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_reduce() -> std::io::Result<()> {
        let reduce = ReduceFunction::new("tmp", "statistic", 0, 2, 1);
        let _res = reduce.reduce();
        Ok(())
    }
}
