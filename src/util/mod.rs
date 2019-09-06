use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::SeekFrom;
use std::path::Path;

///
/// the Splite Postion of the total file
///
/// This struct is used to mark the start and end byte of
/// big url file. We will use multi thread map to
///
#[derive(Clone)]
pub struct FileOffset {
    start: u64,
    end: u64,
}

impl FileOffset {
    pub fn new(start: u64, end: u64) -> Self {
        FileOffset { start, end }
    }
    pub fn get_start(&self) -> u64 {
        self.start
    }
    pub fn get_end(&self) -> u64 {
        self.end
    }
}
pub fn io_splite<P: AsRef<Path>>(path: P, n: usize) -> std::io::Result<Vec<FileOffset>> {
    let metadata = std::fs::metadata(path.as_ref())?;
    let tlen = metadata.len();
    let elen = tlen / (n as u64);

    let f = match File::open(path) {
        Ok(file) => file,
        _ => panic!("Error Open Data File"),
    };

    let mut offset = vec![];
    offset.push(0);
    let mut bffreader = BufReader::with_capacity(4 << 20, f);
    let mut real_next;

    for _i in 1..n {
        let tmp_res = bffreader.seek(SeekFrom::Start(elen as u64));
        if tmp_res.is_err() {
            break;
        }
        let mut end_of_last_url = vec![];
        let tail = bffreader.read_until(b'\n', &mut end_of_last_url).unwrap() as u64;
        bffreader.seek(SeekFrom::Start(tail))?;
        real_next = tmp_res.unwrap() + tail as u64;
        offset.push(real_next);
    }

    let mut offset_set = vec![];
    for i in 0..n {
        if i != n - 1 {
            offset_set.push(FileOffset::new(offset[i], offset[i + 1]));
        } else {
            offset_set.push(FileOffset::new(offset[i], tlen));
        }
    }
    Ok(offset_set)
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_io_splite_read_all() -> std::io::Result<()> {
        let filepath = String::from("urlfile.txt");
        let file_offs = io_splite(&filepath, 2)?;
        let f = File::open(&filepath)?;
        for (index, file_off) in file_offs.iter().enumerate() {
            let mut buf = BufReader::new(f.try_clone()?);
            let sz = file_off.get_end() - file_off.get_start();
            let _res = buf.seek(SeekFrom::Start(file_off.get_start() as u64));
            let mut buffer = vec![0; sz as usize];
            buf.read_exact(&mut buffer)?;
            if index == 0 {
                assert_eq!(b"urlone\r\nurltwo\r\nurlthree\r\n", buffer.as_slice())
            } else {
                assert_eq!(b"urlone\r\n", buffer.as_slice())
            }
        }

        Ok(())
    }

    #[test]
    fn test_io_splite_line() -> std::io::Result<()> {
        let filepath = String::from("urlfile.txt");
        let file_offs = io_splite(&filepath, 2)?;
        let f = File::open(&filepath)?;
        for (index, file_off) in file_offs.iter().enumerate() {
            let buf = BufReader::new(f.try_clone()?);
            let mut sz = file_off.get_end() - file_off.get_start();
            for (index, line) in buf.lines().enumerate() {
                let sten: String = line.unwrap();
                sz -= sten.len() as u64;
                if index == 0 {
                    assert_eq!(sten, "urlone");
                } else if index == 1 {
                    assert_eq!(sten, "urltwo");
                }
                if sz == 0 {
                    break;
                }
            }
        }
        Ok(())
    }
}
