use super::util::BUF_SIZE;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

///
/// This class is for CompareElement in for binary heap
/// Every time it is poped from the priority queue. It
/// will read the next line can give the next big amount.
///
///
struct CompareElement {
    /// the number of most url in the current file
    current_top_num: u64,
    /// the url itself
    current_top_url: String,
    /// read the url of the file each one in a line
    bufreader: BufReader<File>,
}
impl PartialEq for CompareElement {
    fn eq(&self, other: &CompareElement) -> bool {
        self.current_top_num == other.current_top_num
    }
}
impl PartialOrd for CompareElement {
    fn partial_cmp(&self, other: &CompareElement) -> Option<Ordering> {
        if self.current_top_num > other.current_top_num {
            Some(Ordering::Greater)
        } else if self.current_top_num == other.current_top_num {
            Some(Ordering::Equal)
        } else {
            Some(Ordering::Less)
        }
    }
}

impl Eq for CompareElement {}
impl Ord for CompareElement {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
impl CompareElement {
    fn new<P: AsRef<Path>>(file_name: P) -> Self {
        let f = match File::open(file_name) {
            Ok(file) => file,
            _ => panic!("Fail at reading statistic File"),
        };
        let mut bufreader = BufReader::with_capacity(BUF_SIZE, f);
        let mut line = String::new();
        let _res = bufreader.read_line(&mut line);
        let (current_top_num, current_top_url): (u64, String) = if !line.is_empty() {
            let mut iter = line.split_whitespace();
            (
                iter.next().unwrap().parse().unwrap(),
                iter.next().unwrap().to_string(),
            )
        } else {
            (0u64, String::from(""))
        };
        Self {
            current_top_num,
            current_top_url,
            bufreader,
        }
    }

    fn pop(&mut self) -> (u64, String) {
        let ans = (self.current_top_num, self.current_top_url.clone());
        let mut line = String::new();
        let _read_size = match self.bufreader.read_line(&mut line) {
            Ok(size) => size as i64,
            _ => {
                self.current_top_num = 0;
                self.current_top_url = String::from("");
                -1
            }
        };

        if _read_size != -1 && !line.is_empty() {
            let mut iter = line.split_whitespace();
            self.current_top_num = iter.next().unwrap().parse().unwrap();
            self.current_top_url = iter.next().unwrap().to_string();
        }

        ans
    }
}

pub fn get_top_k<P: AsRef<Path> + Clone>(
    prefix: P,
    bucket_number: usize,
    k: usize,
) -> Vec<(u64, String)> {
    let mut heap = BinaryHeap::<CompareElement>::new();
    for i in 0..bucket_number {
        let path = prefix.as_ref().join(i.to_string());
        let ce = CompareElement::new(path.as_path());
        heap.push(ce);
    }

    let mut ans_vec = vec![];
    for _i in 0..k {
        let mut answer = heap.pop().unwrap();
        ans_vec.push((answer.current_top_num, answer.current_top_url.clone()));
        answer.pop();
        heap.push(answer);
    }
    ans_vec
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_get_top_k() {
        let ans = get_top_k("statistic", 2, 3);
        println!("{:?}", ans);
        assert_eq!(
            vec![
                (3, String::from("\"urlone\"")),
                (2, String::from("\"urlfive\"")),
                (1, String::from("\"urlthrea\""))
            ],
            ans
        );
    }
}
