use crate::uri_tools::QueryEncoder;

#[derive(Debug, Clone)]
pub(crate) enum Op {
    LISTSTATUS,
    GETFILESTATUS,
    OPEN,
    CREATE,
    APPEND,
    CONCAT,
    MKDIRS,
    RENAME,
    CREATESYMLINK,
    DELETE
}

impl Op {
    pub(crate) fn op_string(&self) -> &'static str {
        use self::Op::*;
        match self {
            LISTSTATUS => "LISTSTATUS",
            GETFILESTATUS => "GETFILESTATUS",
            OPEN => "OPEN",
            CREATE => "CREATE",
            APPEND => "APPEND",
            CONCAT => "CONCAT",
            MKDIRS => "MKDIRS",
            RENAME => "RENAME",
            CREATESYMLINK => "CREATESYMLINK",
            DELETE => "DELETE"
        }
    }
}

/// Operation argument
#[derive(Debug, Clone)]
pub(crate) enum OpArg {
    /// `[&offset=<LONG>]`
    Offset(i64),
    /// `[&length=<LONG>]`
    Length(i64),
    /// `[&buffersize=<INT>]`
    BufferSize(i32),
    /// `[&overwrite=<true |false>]`
    Overwrite(bool),
    /// `[&blocksize=<LONG>]`
    Blocksize(i64),
    /// `[&replication=<SHORT>]`
    Replication(i16),
    /// `[&permission=<OCTAL>]`
    Permission(u16),
    /// `&sources=<PATHS>`
    Sources(Vec<String>),
    /// `&destination=<PATH>`
    Destination(String),
    /// `[&createParent=<true|false>]`
    CreateParent(bool),
    /// `[&recursive=<true|false>]`
    Recursive(bool)
}

impl OpArg {
    /// add to an url's query string
    pub(crate) fn add_to_url(&self, qe: QueryEncoder) -> QueryEncoder {
        use self::OpArg::*;
        match self {
            Offset(v) => qe.add_pi("offset", *v),
            Length(v) => qe.add_pi("length", *v),
            BufferSize(v) => qe.add_pi("buffersize", *v as i64),
            Overwrite(v) => qe.add_pb("overwrite", *v),
            Blocksize(v) => qe.add_pi("blocksize", *v),
            Replication(v) => qe.add_pi("replication", *v as i64),
            Permission(v) => qe.add_po("permission", *v),
            Sources(v) => qe.add_pv("sources", &v.join(",")),
            Destination(v)=> qe.add_pv("destination", v),
            CreateParent(v) => qe.add_pb("createParent", *v),
            Recursive(v) => qe.add_pb("recursive", *v),
        }
    }
}

macro_rules! opt {
    ($tag:ident, $tp:ty, $op_tag:ident) => {
        pub fn $tag(mut self, v:$tp) -> Self { self.o.push(OpArg::$op_tag(v)); self }
    };
}

/// Define option setters in the option builder
macro_rules! opts {
    // `[&offset=<LONG>]`
    (offset) => { opt! { offset, i64, Offset } };
    // `[&length=<LONG>]`
    (length) => { opt! { length, i64, Length } };
    // `[&overwrite=<true |false>]`
    (overwrite) =>  { opt! { overwrite, bool, Overwrite } };
    // `[&blocksize=<LONG>]`
    (blocksize) => { opt! { blocksize, i64, Blocksize } };
    // `[&replication=<SHORT>]`
    (replication) => { opt! { replication, i16, Replication } };
    // `[&permission=<OCTAL>]`
    (permission) => { opt! { permission, u16, Permission } };
    // `[&buffersize=<INT>]`
    (buffersize) => { opt! { buffersize, i32, BufferSize } };
    // `[&createParent=<true|false>]`
    (create_parent) => { opt! { create_parent, bool, CreateParent } };
    // `[&recursive=<true|false>]`
    (recursive) => { opt! { recursive, bool, Recursive } };
}

macro_rules! op_builder {
    ($tag:ident => $($op:ident),+) => {
        #[derive(Clone)] pub struct $tag { o: Vec<OpArg> }
        impl $tag { 
            pub fn new() -> Self { Self { o: vec![] } }
            pub(crate) fn into(self) -> Vec<OpArg> { self.o }
            $( opts!{$op} )+
        }
    };
}

//curl -i -L "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN
//                    [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]"
op_builder! { OpenOptions => offset, length, buffersize }

//curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
//           [&overwrite=<true |false>][&blocksize=<LONG>][&replication=<SHORT>]
//           [&permission=<OCTAL>][&buffersize=<INT>]"
op_builder! { CreateOptions => overwrite, blocksize, replication, permission, buffersize }

//curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]"
op_builder! { AppendOptions => buffersize }

//curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=MKDIRS[&permission=<OCTAL>]"
op_builder! { MkdirsOptions => permission }

//curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATESYMLINK
//                      &destination=<PATH>[&createParent=<true|false>]"
op_builder! { CreateSymlinkOptions => create_parent }

//curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE
//                      [&recursive=<true|false>]"
op_builder! { DeleteOptions => recursive }