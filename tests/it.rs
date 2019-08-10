//Integration test for webhdfs-rs

use webhdfs::{*, sync_client::*};
//use ReadHdfsFile;

use std::fs::{File, read};
use std::path::Path;
use std::io::{Read, Write, Seek, SeekFrom, BufRead, BufReader};
use std::collections::HashMap;
use std::convert::TryInto;


#[test]
fn webhdfs_test() {
    println!("Integration test -- start");

    //let do_itt = if let Ok(..) = std::env::var("WEBHDFS_BYPASS_ITT") { false } else { true };
    //fn run_shell(cmdline: &str, msg: &'static str) {
    //    use std::process::Command;
    //    assert!(Command::new("bash").arg("-c").arg(cmdline).status().expect("could not run bash").success(), msg)
    //}
    //if do_itt { run_shell("./itt.sh --prepare", "Could not prepare"); }

    fn file_as_string(path: &str) -> String {
        String::from_utf8_lossy(&read(path).expect("cannot file-as-stirng")).to_owned().to_string()
    }

    let entrypoint = file_as_string("./test-data/entrypoint");
    let readscript = file_as_string("./test-data/readscript");
    let writescript = file_as_string("./test-data/writescript");
    let source = file_as_string("./test-data/source");
    let target = file_as_string("./test-data/target");
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
readscript='{r}'
target='{t}'
writescript='{w}'
natmap={n:?}", 
e=entrypoint, s=source, r=readscript, t=target, w=writescript, n=natmap);

    let nm = NatMap::new(natmap.into_iter()).expect("cannot build natmap");
    let entrypoint_uri = "http://".to_owned() + &entrypoint;
    let cx = SyncHdfsClientBuilder::new(entrypoint_uri.parse().expect("Cannot parse entrypoint"))
        .natmap(nm)
        .user_name("root".to_owned())
        .build()
        .expect("cannot HdfsContext::new");

    let (source_dir, source_sfn) = source.split_at(source.rfind('/').expect("source does not contain '/'"));
    let (_, source_fn) = source_sfn.split_at(1);

    //------------------------------------------------
    //Test directory listing
    println!("Test dir and stat");

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

    println!("Read test");

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

    let p = readscript.split(' ').filter(|e| !e.is_empty()).map(|s|{
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
    print!("alloc_mb(len={})...", master_buffer_size);
    let mut b = Vec::with_capacity(master_buffer_size);
    b.resize(master_buffer_size, 0);
    println!("done");

    let mut file = ReadHdfsFile::open(cx, source.clone()).unwrap();

    for op in p {
        println!("{:?}...", op);
        match op {
            Op::Seek(o) => { 
                file.seek(SeekFrom::Start(o.try_into().unwrap())).unwrap(); 
            }
            Op::Read(l, f) => {
                let length: usize = l.try_into().unwrap();
                let readcount = file.read(&mut b[0..length]).unwrap();
                assert_eq!(length, readcount);
                let writecount = File::create(&Path::new(&f)).unwrap().write(&b[0..length]).unwrap();
                assert_eq!(length, writecount);
            }

        }
    }

    let (c,_,_) = file.into_parts();

    println!("Write test");
    let files = writescript.split(' ').filter(|e| !e.is_empty()).collect::<Vec<&str>>();
    let mut file = WriteHdfsFile::create(c, target.clone(), CreateOptions::new(), AppendOptions::new()).unwrap();
    let mut count = 0usize;

    for file_name in files {
        println!("{}", file_name);
        let fb = read(Path::new(&file_name)).expect("couldn't read wseg");
        count += file.write(&fb).unwrap();
    }

    assert_eq!(count, size as usize);

    //if do_itt { run_shell("./itt.sh --validate", "Validation failed"); }

}