//Integration test for webhdfs-rs

use webhdfs::{*, sync_client::*};
//use ReadHdfsFile;

use std::fs::{File, read};
use std::path::Path;
use std::io::{Read, Write, Seek, SeekFrom, BufRead, BufReader};
use std::collections::HashMap;
use std::convert::TryInto;

#[test]
fn test_read() {
    println!("Integration test -- start");

    fn file_as_string(path: &str) -> String {
        String::from_utf8_lossy(&read(path).expect("cannot file-as-stirng")).to_owned().to_string()
    }

    let entrypoint = file_as_string("./test-data/entrypoint");
    let program = file_as_string("./test-data/program");
    let source = file_as_string("./test-data/source");
    let size = file_as_string("./test-data/size").parse::<i64>().unwrap();


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
    let entrypoint_uri = "http://".to_owned() + &entrypoint;
    let cx = SyncHdfsClient::new(entrypoint_uri.parse().expect("Cannot parse entrypoint"), nm).expect("cannot HdfsContext::new");

    let (source_dir, source_sfn) = source.split_at(source.rfind('/').expect("source does not contain '/'"));
    let (_, source_fn) = source_sfn.split_at(1);

    //------------------------------------------------
    //Test directory listing

    //Ok(ListStatusResponse { file_statuses: FileStatuses { file_status: [FileStatus { 
    // access_time: 1564409836087, block_size: 134217728, group: "hadoop", length: 423941508, 
    // modification_time: 1564409849727, owner: "root", path_suffix: "soc-pokec-relationships.txt", 
    // permission: "644", replication: 3, type_: "FILE" 
    //}] } })
    let dir_resp = cx.dir(source_dir);
    println!("Dir: {:?}", dir_resp);
    assert_eq!(source_fn, dir_resp.unwrap().file_statuses.file_status[0].path_suffix);

    let stat_resp = cx.stat(&source);
    println!("Stat: {:?}", stat_resp);
    assert_eq!(size, stat_resp.unwrap().file_status.length);

    //Parse program
    #[derive(Debug)]
    enum Op {
        Seek(i64),
        Read(i64, String)
    }

    fn parse_size(s: &str) -> i64 {
        if s.ends_with("k") {
            &s[0..s.len()-1].parse::<i64>().unwrap() * 1024
        } else if s.ends_with("m") {
            &s[0..s.len()-1].parse::<i64>().unwrap() * 1024 * 1024
        } else {
            s.parse().unwrap()
        }
    }

    let p = program.split(' ').filter(|e| !e.is_empty()).map(|s|{
        let mut i = s.split(':');
        let optype = i.next().unwrap();
        let arg = parse_size(i.next().unwrap());
        match optype {
            "s" => Op::Seek(arg),
            "r" => Op::Read(arg, i.next().unwrap().to_owned()),
            _ => panic!("invalid optype '{}'", s)
        }
    }).collect::<Vec<Op>>();

    //allocate and initialize a large master buffer
    let master_buffer_size = p.iter().map(|w| if let Op::Read(len, _) = w { *len } else { 0 }).max().unwrap().try_into().unwrap();
    let mut b = Vec::with_capacity(master_buffer_size);
    b.resize(master_buffer_size, 0);

    let mut read = ReadHdfsFile::open(cx, source.clone()).unwrap();

    for op in p {
        println!("{:?}...", op);
        match op {
            Op::Seek(o) => { 
                read.seek(SeekFrom::Start(o.try_into().unwrap())).unwrap(); 
            }
            Op::Read(l, f) => {
                let length: usize = l.try_into().unwrap();
                let readcount = read.read(&mut b[0..length]).unwrap();
                assert_eq!(length, readcount);
                let writecount = File::create(&Path::new(&f)).unwrap().write(&b[0..length]).unwrap();
                assert_eq!(length, writecount);
            }

        }
    }


}