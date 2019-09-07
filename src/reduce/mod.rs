use super::util::BUF_SIZE;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::PathBuf;
///
/// The File Reduce Function
///
/// Reduce procedure read url from bucket file that Map just create.
/// It will use hashmap to give a count to each url. Then it sort the urls
/// by the number we just counting. And finnaly we store it into the file.
///
/// *[`Clone`]
///
#[derive(Clone)]
pub struct ReduceFunction {
    /// the output of map operator For a prefix we add the the index of thead. We get the director
    /// that store the url.
    input_prefix: String,
    /// We output sorted url int the form of (number of url, url).
    output_prefix: String,
    /// the bucket each thead process
    bucket_start: usize,
    bucket_end: usize,
    /// the number of threads that previous step takes
    pre_thread_num: usize,
}

impl ReduceFunction {
    pub fn new(
        input_prefix: String,
        output_prefix: String,
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
            let mut path_buf = PathBuf::new();
            path_buf.push(self.output_prefix.as_str());
            path_buf.push(i.to_string().as_str());
            let outpath = path_buf.as_path();

            match std::fs::create_dir(self.output_prefix.as_str()) {
                Ok(()) => println!("Create reduce out file success"),
                _ => println!("Reduce File Director Already_Have"),
            };
            let mut hmap = HashMap::<String, u64>::new();

            for j in 0..self.pre_thread_num {
                let mut strbuf = String::new();
                strbuf.push_str(self.input_prefix.as_str());
                strbuf.push_str(j.to_string().as_str());
                let prefixbuf = PathBuf::from(strbuf);

                let inputfile = prefixbuf.as_path().join(i.to_string().as_str());
                let f = match File::open(inputfile) {
                    Ok(file) => file,
                    _ => panic!("Error Open Reduce File"),
                };

                let bucket_reader = BufReader::with_capacity(BUF_SIZE, f);

                for line in bucket_reader.lines() {
                    let url = line.unwrap();
                    *hmap.entry(url).or_default() += 1;
                }
            }

            let mut url_vec = vec![];
            for (url, num) in hmap.drain() {
                url_vec.push((num, url));
            }
            url_vec.sort_unstable_by(|a, b| a.0.cmp(&b.0).reverse());

            let out_file = File::create(outpath)?;

            let mut bffwriter = BufWriter::with_capacity(BUF_SIZE, out_file);
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
        let reduce = ReduceFunction::new(String::from("tmp"), String::from("statistic"), 0, 2, 2);
        let _res = reduce.reduce();
        Ok(())
    }
}
