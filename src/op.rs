use crate::uri_tools::QueryEncoder;

#[derive(Debug, Clone)]
pub(crate) enum Op {
    LISTSTATUS,
    GETFILESTATUS,
    OPEN,
    CREATE,
    APPEND
}

impl Op {
    pub(crate) fn op_string(&self) -> &'static str {
        match self {
            Op::LISTSTATUS => "LISTSTATUS",
            Op::GETFILESTATUS => "GETFILESTATUS",
            Op::OPEN => "OPEN",
            Op::CREATE => "CREATE",
            Op::APPEND => "APPEND"
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
    Permission(u16)
}

impl OpArg {
    /// add to an url's query string
    pub(crate) fn add_to_url(&self, qe: QueryEncoder) -> QueryEncoder {
        match self {
            OpArg::Offset(v) => qe.add_pi("offset", *v),
            OpArg::Length(v) => qe.add_pi("length", *v),
            OpArg::BufferSize(v) => qe.add_pi("buffersize", *v as i64),
            OpArg::Overwrite(v) => qe.add_pb("overwrite", *v),
            OpArg::Blocksize(v) => qe.add_pi("blocksize", *v),
            OpArg::Replication(v) => qe.add_pi("replication", *v as i64),
            OpArg::Permission(v) => qe.add_po("permission", *v),
        }
    }
}

macro_rules! opt {
    ($tag:ident, $tp:ty, $op_tag:ident) => {
        pub fn $tag(mut self, v:$tp) -> Self { self.o.push(OpArg::$op_tag(v)); self }
    };
}

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


