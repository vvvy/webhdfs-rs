use webhdfs::*;

fn main() {
    use std::fs::File;
    use std::path::Path;
    use std::fs::create_dir_all;
    use commandline::*;
    let (mut client, op) = parse_command_line();

    match op {
        Operation::Get(mut fs) => {
            match &fs[..] {
                &[ref input] => {
                    let input_path = Path::new(input);
                    let output = input_path.file_name().expect2("file name must be specified if no output file is given");
                    let mut out = File::create(&output).expect2("Could not create output file");
                    client.get_file(&input, &mut out).expect2("get error")
                }
                &[ref input, ref output] => {
                    let mut out = File::create(&output).expect2("Could not create output file");
                    client.get_file(&input, &mut out).expect2("get error")
                }
                _ => {
                    let target_dir_ = fs.pop().unwrap();
                    let target_dir = Path::new(&target_dir_);
                    create_dir_all(&target_dir).expect2("Could not create output dir");
                    for input in fs {
                        let input_path = Path::new(&input);
                        let output_file = input_path.file_name().expect2("file name must be specified if no output file is given");
                        let output = target_dir.join(&Path::new(output_file));
                        let mut out = File::create(&output).expect2("Could not create output file");
                        client.get_file(&input, &mut out).expect2("get error")
                    }
                    
                }
            }
        }
    }
}

fn version() -> ! {
    println!(
        "{} ({}) version {}",
        env!("CARGO_PKG_DESCRIPTION"),
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );
    std::process::exit(0);
}

fn usage() -> ! {
    println!("USAGE:
webhdfs <options>... <command> <files>...
webhdfs -h|--help
webhdfs -v|--version

options:

    -U|--uri|--url <url>        API entrypoint
    -u|--user <string>          User name
    -d|--doas <string>          DoAs username
    -T|--dt <string>            Delegation token
    -t|--timeout <unsigned>     Default timeout in seconds
    -N|--natmap-file <filepath> Path to NAT mappings file
    -n|--natmap-entry <k=v>     NAT mapping (multiple options are Ok)

command and files:
    -v|--version                   
        Print version and exit

    -h|--help
        Print this thelp screen and exit

    --save-config <filepath>
        Save the effective configuration to the file

    -g|--get <remote-filepath> <local-path>
    -g|--get <remote-filepath>
    -g|--get <remote-filepath>.. <local-dirpath>
        Get files from HDFS

");
    std::process::exit(1);
}

enum Operation {
    Get(Vec<String>)
}


fn parse_command_line() -> (SyncHdfsClient, Operation) {
    use std::time::Duration;
    use std::collections::HashMap;
    use commandline::*;

    enum Sw {
        Uri, User, Doas, DToken, Timeout, NMFile, NMEntry, SaveConfig
    }
    enum Op {
        Get
    }
    struct S {
        sw: Option<Sw>,
        op: Option<Op>,
        files: Vec<String>,
        uri: Option<String>,
        user: Option<String>,
        doas: Option<String>,
        dtoken: Option<String>,
        timeout: Option<Duration>,
        natmap: Option<HashMap<String, String>>,
        save_config: Option<String>,
    }

    let s0 = S { 
        sw: None, op: None, files: vec![], 
        uri: None, user: None, doas:None, timeout: None, dtoken: None, natmap: None,
        save_config: None 
    };

    let result = commandline::parse_cmdln(s0, |mut s, arg| if let Some(sw) = s.sw.take() {
        match sw {
            Sw::Uri => S { uri: Some(arg.arg()), ..s },
            Sw::User => S { user: Some(arg.arg()), ..s },
            Sw::Doas => S { doas: Some(arg.arg()), ..s },
            Sw::DToken => S { dtoken: Some(arg.arg()), ..s },
            Sw::SaveConfig => S { save_config: Some(arg.arg()), ..s },
            Sw::Timeout => S { timeout: Some(Duration::from_secs(arg.arg().parse().expect2("Invalid timeout duration"))), ..s },
            Sw::NMFile => S { natmap: Some(config::read_kv_file(&arg.arg()).expect2("malformed natmap file")), ..s },
            Sw::NMEntry =>  { 
                let mut nm = if let Some(nm) = s.natmap { nm } else { HashMap::new() };
                let (k, v) = config::split_kv(arg.arg()).expect2("invalid natmap entry");
                nm.insert(k, v);
                S { natmap: Some(nm), ..s }
            }
        }
    } else {
        match arg.switch_ref() {
            "-v"|"--version" => version(),
            "-h"|"--help" => usage(),
            "-g"|"--get" => S { op: Some(Op::Get), ..s },
            "-U"|"--uri"|"--url" => S { sw: Some(Sw::Uri), ..s },
            "-u"|"--user" => S { sw: Some(Sw::User), ..s },
            "-d"|"--doas" => S { sw: Some(Sw::Doas), ..s },
            "-T"|"--dt" => S { sw: Some(Sw::DToken), ..s },
            "-t"|"--timeout" => S { sw: Some(Sw::Timeout), ..s },
            "-N"|"--natmap-file" => S { sw: Some(Sw::NMFile), ..s },
            "-n"|"--natmap-entry" => S { sw: Some(Sw::NMEntry), ..s },
            "--save-config" => S { sw: Some(Sw::SaveConfig), ..s },
            _ => { s.files.push(arg.arg()); s}
        }
    });

    if result.sw.is_some() {
        error_exit("invalid command line at the end", "")
    }

    if let Some(f) = result.save_config {
        if result.op.is_some() {
            error_exit("--save-config must be used alone", "")
        }
        let uri = result.uri.expect2("must specify --uri when saving config");
        let cfg = config::Config::new(uri.parse().expect2("Cannot parse URI"));
        config::write_config(&std::path::Path::new(&f), &cfg, true);
        std::process::exit(0);
    } else {
        let operation = if let Some(op) = result.op {
            op
        } else {
            error_exit("must specify operation", "")
        };

        //build context
        let mut cx = if let Some(uri) = result.uri { 
            SyncHdfsClientBuilder::new(uri.parse().expect2("Cannot parse URI")) 
        } else { 
            SyncHdfsClientBuilder::from_config_opt().expect2("No configuration files were found, and no mandatory options (--uri) were specified")
        };
        if let Some(user) = result.user { cx = cx.user_name(user) }
        if let Some(doas) = result.doas { cx = cx.doas(doas) }
        if let Some(timeout) = result.timeout { cx = cx.default_timeout(timeout) }
        if let Some(natmap) = result.natmap { cx = cx.natmap(NatMap::new(natmap.into_iter()).expect2("Invalid natmap")) }
        if let Some(dtoken) = result.dtoken { cx = cx.delegation_token(dtoken) }
        let client = cx.build().expect2("Cannot build SyncHdfsClient");

        let operation = match operation {
            Op::Get =>
                if result.files.len() > 0 { Operation::Get(result.files) } else { error_exit("must specify at least one input file for --get", "") }
        };

        (client, operation)
    }
}



//-------------------------


mod commandline {
    


    /// Prints two-part message to stderr and exits
    pub fn error_exit(msg: &str, detail: &str) -> ! {
        eprint!("Error: {}", msg);
        if detail.is_empty() {
            eprintln!()
        } else {
            eprintln!(" ({})", detail);
        }
        std::process::exit(1)
    }

    /// Expect2 function
    pub trait Expect2<T> {
        /// Same as Result::expect but the error message is brief and not intimidating
        fn expect2(self, msg: &str) -> T;
    }

    impl<T, E: std::error::Error> Expect2<T> for std::result::Result<T, E> {
        fn expect2(self, msg: &str) -> T {
            match self {
                Ok(v) => v,
                Err(e) => error_exit(msg, &e.to_string())
            }
        }
    }

    impl<T> Expect2<T> for Option<T> {
        fn expect2(self, msg: &str) -> T {
            match self {
                Some(v) => v,
                None => error_exit(msg, "")
            }
        }
    }

    #[derive(Debug)]
    pub enum CmdLn {
        Switch(String),
        Arg(String),
        Item(String)
    }

    impl std::fmt::Display for CmdLn {
        fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                CmdLn::Switch(s) => write!(fmt, "Switch '{}'", s),
                CmdLn::Arg(s) => write!(fmt, "Arg '{}'", s),
                CmdLn::Item(s) => write!(fmt, "Item '{}'", s)
            }
        }
    }

    impl CmdLn {
        /// Splits command line argruments if needed
        /// - _ if bypass => Item(_)
        /// - '--sw=arg' => Switch('--sw') Arg('arg')
        /// - '-abc' => Item('-a') Item('-b') Item('-c')
        /// - '--' => *bypass = true; []
        /// - _ => Item(_)
        fn convert_arg(bypass: &mut bool, v: String) -> Vec<CmdLn> {
            use std::iter::FromIterator;
            if *bypass {
                vec![CmdLn::Item(v)]
            } else if v == "--" {
                *bypass = true;
                vec![]
            } else if v.starts_with("--") {
                let mut s: Vec<String> = v.splitn(2, "=").map(|r| r.to_string()).collect();
                let a = s.pop();
                let b = s.pop();
                match (a, b) {
                    (Some(a), None) => vec![CmdLn::Item(a)],
                    (Some(b), Some(a)) => vec![CmdLn::Switch(a), CmdLn::Arg(b)],
                    _ => unreachable!()
                }
            } else if v.starts_with("-") && v != "-" {
                v.chars().skip(1).map(|c| CmdLn::Item(String::from_iter(vec!['-', c]))).collect()
            } else {
                vec![CmdLn::Item(v)]
            }
        }

        fn raise(&self, w: &str) -> ! {
            error_exit(&format!("we wanted {}, but got {:?}", w, self), "command line syntax error")
        }

        /*pub fn switch(self) -> String {
            match self {
                CmdLn::Switch(v) | CmdLn::Item(v) => v,
                other => other.raise("Switch")
            }
        }*/

        pub fn switch_ref(&self) -> &str {
            match self {
                CmdLn::Switch(v) | CmdLn::Item(v) => v,
                other => other.raise("Switch")
            }
        }

        pub fn arg(self) -> String {
            match self {
                CmdLn::Arg(v) | CmdLn::Item(v) => v,
                other => other.raise("Arg")
            }
        }
    }

    /// Parses command line for 0- and 1-argument options.
    /// `f` consumes the current state and a command line item, and produces the new state.
    pub fn parse_cmdln<S, F>(s0: S, f: F) -> S where F: FnMut(S, CmdLn) -> S {
        std::env::args().skip(1).scan(false, |s, a| Some(CmdLn::convert_arg(s, a))).flatten().fold(s0, f)
    }

    /*
    pub fn bool_opt(s: String) -> bool {
        match s.as_ref() {
            "true"|"+"|"yes" => true,
            "false"|"-"|"no" => false,
            v => panic!("invalid bool value '{}'", v)
        }
    }
    */
}