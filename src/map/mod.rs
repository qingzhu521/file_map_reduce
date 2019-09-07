use super::util::BUF_SIZE;
use crate::util::FileOffset;

use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
///
/// The File Map Function
///
/// Map function will be work in each thread. And each Map function
/// will read it's own part of total file that io_splite gives.
///
/// After reading it will hash every "url" and use hash value moduling the
/// value defined by the user. Then the Map function will store each url into
/// the file bucket numbered by moduler that just caculated.
///
/// *[`Clone`]
///
#[derive(Clone)]
pub struct MapFunction {
    file_offset: FileOffset,
    input_file_name: String,
    output_dir: String,
    bucket_num: usize,
}

impl MapFunction {
    pub fn new(
        file_offset: FileOffset,
        input_file_name: String,
        output_dir: String,
        bucket_num: usize,
    ) -> Self {
        Self {
            file_offset,
            input_file_name,
            output_dir,
            bucket_num,
        }
    }

    pub fn map(&mut self) -> std::io::Result<()> {
        let f = match File::open(self.input_file_name.as_str()) {
            Ok(file) => file,
            _ => panic!("Error Open Map File"),
        };
        let mut bffreader = BufReader::with_capacity(BUF_SIZE, f);
        let _res = bffreader.seek(SeekFrom::Start(self.file_offset.get_start()));
        let mut bffwriter_vec = vec![];
        match std::fs::create_dir(self.output_dir.as_str()) {
            Ok(()) => println!("Create OK"),
            _ => println!("Map output director Already have"),
        };

        for i in 0..self.bucket_num {
            let mut path_buf = PathBuf::from(self.output_dir.as_str());
            path_buf.push(&i.to_string());
            let output_file = path_buf.as_path();

            let f = File::create(output_file)?;
            bffwriter_vec.push(BufWriter::with_capacity(BUF_SIZE, f));
        }
        let mut sz = self.file_offset.get_end() - self.file_offset.get_start();
        for line in bffreader.lines() {
            let mut url: String = line.unwrap();

            let line_sep = if cfg!(target_os = "windows") { 2 } else { 1 };
            sz -= (url.len() + line_sep) as u64; // And the \n

            let mut hasher = DefaultHasher::new();
            url.hash(&mut hasher);
            let hash_res = hasher.finish();

            let output_index = hash_res % (self.bucket_num as u64);
            url.push('\n');
            let _res = bffwriter_vec[output_index as usize].write(url.as_bytes());
            if sz == 0 {
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::super::util::io_splite;
    use super::*;
    use std::iter::Iterator;
    #[test]
    fn test_map() -> std::io::Result<()> {
        let io_spliter = io_splite(String::from("urlfile.txt"), 2).unwrap();

        for (index, fo) in io_spliter.iter().enumerate() {
            let mut prefix = String::from("tmp");
            prefix.push_str(index.to_string().as_str());
            let mut mapper = MapFunction::new(fo.clone(), String::from("urlfile.txt"), prefix, 2);
            mapper.map()?;
        }

        Ok(())
    }

}
