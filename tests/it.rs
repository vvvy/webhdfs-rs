//Integration test for webhdfs-rs

use webhdfs::{*, sync_client::*, config::HttpsConfig};
//use ReadHdfsFile;

use std::time::Duration;
use std::fs::{File, read};
use std::path::Path;
use std::io::{Read, Write, Seek, SeekFrom};
use std::convert::TryInto;




#[test]
fn webhdfs_test() {
    println!("Integration test -- start");
    //env_logger::init();
    let _ = env_logger::builder().is_test(true).try_init();


    //let do_itt = if let Ok(..) = std::env::var("WEBHDFS_BYPASS_ITT") { false } else { true };
    //fn run_shell(cmdline: &str, msg: &'static str) {
    //    use std::process::Command;
    //    assert!(Command::new("bash").arg("-c").arg(cmdline).status().expect("could not run bash").success(), msg)
    //}
    //if do_itt { run_shell("./itt.sh --prepare", "Could not prepare"); }
    
    fn file_as_string(path: &str) -> String {
        String::from_utf8_lossy(&read(path).expect("cannot file-as-stirng")).to_owned().to_string()
    }
    
    fn file_as_string_opt(path: &str) -> Option<String> {
        read(path).map(|s| String::from_utf8_lossy(&s).to_owned().to_string()).ok()
    }

    let entrypoint = file_as_string("./test-data/entrypoint");
    let alt_entrypoint = file_as_string_opt("./test-data/alt-entrypoint");
    let has_alt_entrypoint = alt_entrypoint.is_some();

    let natmap = crate::config::read_kv_file("./test-data/natmap").expect("cannot read natmap");
    let user = file_as_string_opt("./test-data/user");
    let dtoken = file_as_string_opt("./test-data/dtoken");
    let scheme = file_as_string("./test-data/scheme");
    println!("
entrypoint='{e}'
alt_entrypoint='{ae:?}'
natmap={n:?}
user={u:?}
dtoken={d:?}", 
e=entrypoint, ae=alt_entrypoint, n=natmap, u=user, d=dtoken);
    let nm = NatMap::new(natmap.into_iter()).expect("cannot build natmap");
    let mut https_config = HttpsConfig::new();
    https_config.danger_accept_invalid_certs = Some(true);
    https_config.danger_accept_invalid_hostnames = Some(true);
    let entrypoint_uri = format!("{}://{}", scheme, entrypoint);
    let b = SyncHdfsClientBuilder::new(entrypoint_uri.parse().expect("Cannot parse entrypoint"))
        .default_timeout(Duration::from_secs(180))
        .natmap(nm)
        .https_settings(https_config.into());
    let b = if let Some(w) = alt_entrypoint {
        let alt_entrypoint_uri = format!("{}://{}", scheme, w);
        b.alt_entrypoint(alt_entrypoint_uri.parse().expect("Cannot parse alt_entrypoint")) 
    } else { b };
    let b = if let Some(w) = dtoken { b.delegation_token(w) } else { b };
    let b = if let Some(w) = user { b.user_name(w) } else { b };
    let mut cx = b.build().expect("cannot HdfsContext::new");

    let readscript = file_as_string("./test-data/readscript");
    let writescript = file_as_string("./test-data/writescript");
    let source = file_as_string("./test-data/source");
    let target = file_as_string("./test-data/target");
    let size = file_as_string("./test-data/size").parse::<i64>().unwrap();
    println!("
source='{s}'
readscript='{r}'
target='{t}'
writescript='{w}'
size={z}", 
s=source, r=readscript, t=target, w=writescript, z=size);
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
    //assert_eq!(source_fn, dir_resp.unwrap().file_statuses.file_status[0].path_suffix);
    dir_resp.unwrap().file_statuses.file_status.into_iter().find(|fs| fs.path_suffix == source_fn)
    .ok_or("cannot find sourcefile in hdfs")
    .unwrap();

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

    let (cx,_,_) = file.into_parts();

    println!("Write test");
    let files = writescript.split(' ').filter(|e| !e.is_empty()).collect::<Vec<&str>>();
    let mut file = WriteHdfsFile::create(cx, target.clone(), CreateOptions::new(), AppendOptions::new()).unwrap();
    let mut count = 0usize;

    for file_name in files {
        //std::thread::sleep(std::time::Duration::from_secs(3));
        println!("{}", file_name);
        let fb = read(Path::new(&file_name)).expect("couldn't read wseg");
        count += file.write(&fb).unwrap();
    }

    assert_eq!(count, size as usize);

    let (mut cx,_) = file.into_parts();

    //MKDIRS/DELETE(dir) test
    let dir_to_make = file_as_string("./test-data/dir-to-make");
    cx.mkdirs(&dir_to_make, MkdirsOptions::new()).expect("mkdirs");
    let mkdirs_stat_resp = cx.stat(&dir_to_make);
    //println!("Mkdirs Stat: {:?}", mkdirs_stat_resp);
    assert_eq!(dirent_type::DIRECTORY, mkdirs_stat_resp.unwrap().file_status.type_);

    let dir_to_remove= file_as_string("./test-data/dir-to-remove");
    let rmdir_stat_resp = cx.stat(&dir_to_remove);
    //println!("Stat: {:?}", rmdir_stat_resp);
    assert_eq!(dirent_type::DIRECTORY, rmdir_stat_resp.unwrap().file_status.type_);
    cx.delete(&dir_to_remove, DeleteOptions::new()).expect("delete (dir)");
    let x = cx.stat(&dir_to_remove).expect_err("delete(dir) failed");
    println!("{}", x);


    //failover test
    if has_alt_entrypoint {
        println!("Failover test");
        let standby_state = cx.fostate().next();
        let mut cx = cx.with_fostate(standby_state);
        let dir_resp = cx.dir(source_dir);
        println!("Dir: {:?}", dir_resp);
        //assert_eq!(source_fn, dir_resp.unwrap().file_statuses.file_status[0].path_suffix);
        dir_resp.unwrap().file_statuses.file_status.into_iter().find(|fs| fs.path_suffix == source_fn)
        .ok_or("cannot find sourcefile in hdfs")
        .unwrap();

        //do the same again - to make sure the correct state was memoized on the previous step
        //the 1st request should go directly to the active node (consult the logs)
        let dir_resp = cx.dir(source_dir);
        println!("Dir(2): {:?}", dir_resp);
        //assert_eq!(source_fn, dir_resp.unwrap().file_statuses.file_status[0].path_suffix);
        dir_resp.unwrap().file_statuses.file_status.into_iter().find(|fs| fs.path_suffix == source_fn)
        .ok_or("cannot find sourcefile in hdfs")
        .unwrap();

        //TODO: test all other control paths (get/put binary, op_json, op_empty)starting in standby nn state
        //at least read and write (small chunks/files)

        //get binary
        let cx = cx.with_fostate(standby_state);
        let mut file = ReadHdfsFile::open(cx, source.clone()).unwrap();
        let mut b = Vec::with_capacity(1024);
        b.resize(b.capacity(), 0);
        file.read(&mut b).unwrap();
    } else {
        println!("No alt_entrypoint specified -- skip failover test");
    }

    //if do_itt { run_shell("./itt.sh --validate", "Validation failed"); }

}