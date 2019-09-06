use crate::util::FileOffset;
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;

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
pub struct MapFunction<P: AsRef<Path>> {
    file_offset: FileOffset,
    input_file_name: P,
    output_dir: P,
    output_num: usize,
}

impl<P: AsRef<Path>> MapFunction<P> {
    pub fn new(
        file_offset: FileOffset,
        input_file_name: P,
        output_dir: P,
        output_num: usize,
    ) -> Self {
        Self {
            file_offset,
            input_file_name,
            output_dir,
            output_num,
        }
    }

    pub fn map(&self) -> std::io::Result<()> {
        let f = match File::open(self.input_file_name.as_ref()) {
            Ok(file) => file,
            _ => panic!("Error Open Map File"),
        };
        let bffreader = BufReader::with_capacity(1 << 20, f);

        let mut bffwriter_vec = vec![];
        for i in 0..self.output_num {
            let out_file = i.to_string();
            let path_buf = self.output_dir.as_ref().join(&out_file);
            let output_file = path_buf.as_path();
            match std::fs::create_dir(self.output_dir.as_ref()) {
                Ok(()) => println!("Create OK"),
                _ => println!("Already have"),
            };
            let f = File::create(output_file)?;
            bffwriter_vec.push(BufWriter::with_capacity(1 << 20, f));
        }
        let mut sz = self.file_offset.get_end() - self.file_offset.get_start();
        let mut hasher = DefaultHasher::new();
        for line in bffreader.lines() {
            let mut url: String = line.unwrap();
            sz -= url.len() as u64;
            url.hash(&mut hasher);
            let hash_res = hasher.finish();
            let output_index = hash_res % (self.output_num as u64);
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
    use super::*;

    #[test]
    fn test_map() -> std::io::Result<()> {
        use std::fs::metadata;
        let meta = metadata("urlfile.txt")?;
        let len = meta.len();

        let fo = FileOffset::new(0, len);

        let mapper = MapFunction::new(fo, "urlfile.txt", "tmp0", 2);
        mapper.map()?;

        Ok(())
    }
}
