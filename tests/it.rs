//Integration test for webhdfs-rs

use webhdfs::{HdfsContext, NatMap, dir, /*ReadHdfsFile*/};

use std::fs::{File, read};
use std::io::{BufRead, BufReader};
use std::collections::HashMap;

#[test]
fn test_read() {
    println!("Integration test -- start");

    fn file_as_string(path: &str) -> String {
        String::from_utf8_lossy(&read(path).expect("cannot file-as-stirng")).to_owned().to_string()
    }

    let entrypoint = file_as_string("./test-data/entrypoint");
    let program = file_as_string("./test-data/program");
    let source = file_as_string("./test-data/source");

    let f = File::open("./test-data/natmap").expect("cannot open natmap");
    let f = BufReader::new(f);

    let natmap: HashMap<String, String> = f.lines().map(
        |l| {
            let w = l.expect("cannot read natmap line");
            let mut x = w.splitn(2, "=");
            let a = x.next().expect("cannot read natmap line f1").to_owned();
            let b = x.next().expect("cannot read natmap line f2").to_owned();
            (a, b)
        }).collect();

    println!("
entrypoint='{e}'
source='{s}'
program='{p}'
natmap={n:?}", 
e=entrypoint, s=source, p=program, n=natmap);

    let nm = NatMap::new(natmap.into_iter()).expect("cannot build natmap");
    let cx = HdfsContext::new(entrypoint.parse().expect("Cannot parse entrypoint"), nm).expect("cannot HdfsContext::new");

    let (source_dir, _source_sfn) = source.split_at(source.rfind('/').expect("source does not contain '/'"));


    println!("{:?}", dir(&cx, source_dir))

}