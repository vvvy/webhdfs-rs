use std::fmt::{Display, Formatter, Result as FmtResult};
use serde::{Deserialize};

/*
HTTP/1.1 404 Not Found
Content-Type: application/json
Transfer-Encoding: chunked

{
  "RemoteException":
  {
    "exception"    : "FileNotFoundException",
    "javaClassName": "java.io.FileNotFoundException",
    "message"      : "File does not exist: /foo/a.patch"
  }
}
*/

#[derive(Debug, Deserialize)]
pub struct RemoteExceptionResponse {
    #[serde(rename="RemoteException")]
    pub remote_exception: RemoteException
}

#[derive(Debug, Deserialize)]
pub struct RemoteException {
    pub exception: String,
    #[serde(rename="javaClassName")]
    pub java_class_name: String,
    pub message: String
}

impl Display for RemoteException {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, 
            "RemoteException[exception={}, java_class_name={}, msg='{}']", 
            self.exception, self.java_class_name, self.message
        )
    }
}

impl std::error::Error for RemoteException {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { None }
}


/*
{
  "FileStatuses":
  {
    "FileStatus":
    [
      {
        "accessTime"      : 1320171722771,
        "blockSize"       : 33554432,
        "group"           : "supergroup",
        "length"          : 24930,
        "modificationTime": 1320171722771,
        "owner"           : "webuser",
        "pathSuffix"      : "a.patch",
        "permission"      : "644",
        "replication"     : 1,
        "type"            : "FILE"
      },
      {
        "accessTime"      : 0,
        "blockSize"       : 0,
        "group"           : "supergroup",
        "length"          : 0,
        "modificationTime": 1320895981256,
        "owner"           : "username",
        "pathSuffix"      : "bar",
        "permission"      : "711",
        "replication"     : 0,
        "type"            : "DIRECTORY"
      },
      ...
    ]
  }
}
*/

#[derive(Debug, Deserialize)]
pub struct ListStatusResponse {
    #[serde(rename="FileStatuses")]
    pub file_statuses: FileStatuses
}

#[derive(Debug, Deserialize)]
pub struct FileStatuses {
    #[serde(rename="FileStatus")]
    pub file_status: Vec<FileStatus>
}

#[derive(Debug, Deserialize)]
pub struct FileStatus {
    //"accessTime"      : 1320171722771,
    #[serde(rename="accessTime")]
    pub access_time: i64,

    //"blockSize"       : 33554432,
    #[serde(rename="blockSize")]
    pub block_size: i64,

    //"group"           : "supergroup",
    pub group: String,

    //"length"          : 24930,
    pub length: i64,

    //"modificationTime": 1320171722771,
    #[serde(rename="modificationTime")]
    pub modification_time: i64,

    //"owner"           : "webuser",
    pub owner: String,

    //"pathSuffix"      : "a.patch",
    #[serde(rename="pathSuffix")]
    pub path_suffix: String,

    //"permission"      : "644",
    pub permission: String,

    //"replication"     : 1,
    pub replication: i32,

    //"type"            : "FILE"
    #[serde(rename="type")]
    pub type_: String
}

/*
HTTP/1.1 200 OK
Content-Type: application/json
Transfer-Encoding: chunked

{
  "FileStatus":
  {
    "accessTime"      : 0,
    "blockSize"       : 0,
    "group"           : "supergroup",
    "length"          : 0,             //in bytes, zero for directories
    "modificationTime": 1320173277227,
    "owner"           : "webuser",
    "pathSuffix"      : "",
    "permission"      : "777",
    "replication"     : 0,
    "type"            : "DIRECTORY"    //enum {FILE, DIRECTORY, SYMLINK}
  }
}
*/

/// Directory entry types (as returmed by stat and dir)
pub mod dirent_type {
    /// Value of `FileStatus.type` corresponding to a regular file (`"FILE"`)
    pub const FILE: &'static str = "FILE";
    /// Value of `FileStatus.type` corresponding to a directory (`"DIRECTORY"`)
    pub const DIRECTORY: &'static str = "DIRECTORY";
    /// Value of `FileStatus.type` corresponding to a symbolic link (`"SYMLINK"`)
    pub const SYMLINK: &'static str = "SYMLINK";
}

#[derive(Debug, Deserialize)]
pub struct FileStatusResponse {
    #[serde(rename="FileStatus")]
    pub file_status: FileStatus
}

/*
HTTP/1.1 200 OK
Content-Type: application/json
Transfer-Encoding: chunked

{"boolean": true}
*/

/// Response to MKDIRS, DELETE, RENAME, TRUNCATE
#[derive(Debug, Deserialize)]
pub struct Boolean {
    pub boolean: bool
}
