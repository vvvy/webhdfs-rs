enum UriEncodingIteratorState {
    Null,
    C2(u8, u8),
    C1(u8)
}

pub struct UriEncodingIterator<I> {
    s: UriEncodingIteratorState,
    i: I,
    omit_slash: bool
}

impl<I> UriEncodingIterator<I> {
    //bitmap of RFC 3986 unreserved characters, 1 === char should not be encoded
    const UNRESERVED_BM: [u64; 4] = [0x3FF600000000000, 0x47FFFFFE87FFFFFE, 0, 0];

    fn is_unreserved(ch: u8) -> bool { Self::UNRESERVED_BM[ch as usize / 64] & (1 << (ch % 64)) != 0 }

    pub fn new(i: I, omit_slash: bool) -> Self { Self { i, s: UriEncodingIteratorState::Null, omit_slash } }

    fn hex_from_digit(d: u8) -> u8 {
        if d < 10 { b'0' + d } else { b'A' + d - 10 }
    }
}

type SliceUriEncodingIterator<'s> = UriEncodingIterator<std::iter::Cloned<std::slice::Iter<'s, u8>>>;

pub fn uri_part_encoder_iter(arg: &str, omit_slash: bool) -> SliceUriEncodingIterator<'_> {
    UriEncodingIterator::new(arg.as_bytes().iter().cloned(), omit_slash)
}

impl<I> Iterator for UriEncodingIterator<I> where I: Iterator<Item=u8> {
    type Item = u8;
    fn next(&mut self) -> Option<u8> {
        match self.s {
            UriEncodingIteratorState::C2(b0, b1) => {
                self.s = UriEncodingIteratorState::C1(b1);
                Some(b0)
            }
            UriEncodingIteratorState::C1(b) => {
                self.s = UriEncodingIteratorState::Null;
                Some(b)
            }
            UriEncodingIteratorState::Null => match self.i.next() {
                //encode if b is not unreserved, and if (b is slash and we omit slashes) is wrong
                Some(b) if !Self::is_unreserved(b) && !(b == b'/' && self.omit_slash) => {
                    self.s = UriEncodingIteratorState::C2(
                        Self::hex_from_digit((b >> 4) & 0x0f), 
                        Self::hex_from_digit(b & 0x0f)
                        );
                    Some(b'%')
                }
                other => other 
            }
        }
    }
}

#[test]
fn test_uri_encoding_iteator() {
    fn expect_encoded(omit_slash: bool, expected: &str, source: &str) {
        assert_eq!(expected.bytes().collect::<Vec<u8>>(), uri_part_encoder_iter(source, omit_slash).collect::<Vec<u8>>())
    }
    expect_encoded(false, "", "");
    expect_encoded(true,  "", "");

    expect_encoded(false, "az09Az", "az09Az");
    expect_encoded(true,  "az09Az", "az09Az");
    expect_encoded(false, "user%2Fa%2Fb%3A%24ce8xABC%26", "user/a/b:$ce8xABC&");
    expect_encoded(true,  "user/a/b%3A%24ce8xABC%26", "user/a/b:$ce8xABC&");

    expect_encoded(false, "user%2Fa%2F%D0%9A%D0%B8%D1%80%D0%B8%D0%BB%D0%BB%D0%B8%D1%86%D0%B0AndEng%2Fu", "user/a/КириллицаAndEng/u");
    expect_encoded(true,  "user/a/%D0%9A%D0%B8%D1%80%D0%B8%D0%BB%D0%BB%D0%B8%D1%86%D0%B0AndEng/u", "user/a/КириллицаAndEng/u");

    expect_encoded(false, 
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789", 
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    );

    expect_encoded(true,  
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789", 
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    );

    expect_encoded(false,
    "%D0%90%D0%91%D0%92%D0%93%D0%94%D0%95%D0%81%D0%96%D0%97%D0%98%D0%99%D0%9A%D0%9B%D0%9C%D0%9D%D0%9E%D0%9F%D0%A0%D0%A1%D0%A2%D0%A3%D0%A4%D0%A5%D0%A6%D0%A7%D0%A8%D0%A9%D0%AC%D0%AB%D0%AA%D0%AD%D0%AE%D0%AF%D0%B0%D0%B1%D0%B2%D0%B3%D0%B4%D0%B5%D1%91%D0%B6%D0%B7%D0%B8%D0%B9%D0%BA%D0%BB%D0%BC%D0%BD%D0%BE%D0%BF%D1%80%D1%81%D1%82%D1%83%D1%84%D1%85%D1%86%D1%87%D1%88%D1%89%D1%8C%D1%8B%D1%8A%D1%8D%D1%8E%D1%8F",
    "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯабвгдеёжзийклмнопрстуфхцчшщьыъэюя"
    );    
    
    expect_encoded(true,
    "%D0%90%D0%91%D0%92%D0%93%D0%94%D0%95%D0%81%D0%96%D0%97%D0%98%D0%99%D0%9A%D0%9B%D0%9C%D0%9D%D0%9E%D0%9F%D0%A0%D0%A1%D0%A2%D0%A3%D0%A4%D0%A5%D0%A6%D0%A7%D0%A8%D0%A9%D0%AC%D0%AB%D0%AA%D0%AD%D0%AE%D0%AF%D0%B0%D0%B1%D0%B2%D0%B3%D0%B4%D0%B5%D1%91%D0%B6%D0%B7%D0%B8%D0%B9%D0%BA%D0%BB%D0%BC%D0%BD%D0%BE%D0%BF%D1%80%D1%81%D1%82%D1%83%D1%84%D1%85%D1%86%D1%87%D1%88%D1%89%D1%8C%D1%8B%D1%8A%D1%8D%D1%8E%D1%8F",
    "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯабвгдеёжзийклмнопрстуфхцчшщьыъэюя"
    );

    expect_encoded(false, 
    "~%60%21%40%23%24%25%5E%26%2A%28%29_%2B-%3D%7B%7D%7C%5B%5D%5C%3A%22%3B%27%3C%3E%3F%2C.%2F", 
    "~`!@#$%^&*()_+-={}|[]\\:\";'<>?,./");
    expect_encoded(true,  
    "~%60%21%40%23%24%25%5E%26%2A%28%29_%2B-%3D%7B%7D%7C%5B%5D%5C%3A%22%3B%27%3C%3E%3F%2C./", 
    "~`!@#$%^&*()_+-={}|[]\\:\";'<>?,./");
}

#[test]
fn gen_bitmap() {
    //RFC 3986
    let unreserved_chars = [
b'A', b'B', b'C', b'D', b'E', b'F', b'G', b'H', b'I', b'J', b'K', b'L', b'M', b'N', b'O', b'P', b'Q', b'R', b'S', b'T', b'U', b'V', b'W', b'X', b'Y', b'Z',
b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h', b'i', b'j', b'k', b'l', b'm', b'n', b'o', b'p', b'q', b'r', b's', b't', b'u', b'v', b'w', b'x', b'y', b'z',
b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'-', b'_', b'.', b'~'
    ];

    //let reserved_chars = [
    //    b'!', b'*', b'\'', b'(', b')', b';', b':', b'@', b'&', b'=', b'+', b'$', b',', b'/', b'?', b'#', b'[', b']'        
    //];

    let mut bm = [0u64; 4];

    for v in unreserved_chars.iter() {
        let (a, b) = (v / 64, v % 64);
        bm[a as usize] |= 1 << b;
    }

    println!("{:?}", bm.iter().map(|w| format!("{:X}", w)).collect::<Vec<String>>())

    //["3FF600000000000", "47FFFFFE87FFFFFE", "0", "0"]
}


pub struct QueryEncoder {
    path_and_query: Vec<u8>,
    qm_added: bool
}

impl QueryEncoder {
    fn pfx(&mut self) {
        if !self.qm_added {
            self.qm_added = true;
            self.path_and_query.push(b'?');
        } else {
            self.path_and_query.push(b'&');
        }
    }
    /// add arbitrary string (encoding performed)
    pub fn add_pv(mut self, p: &str, v: &str) -> QueryEncoder {
        self.pfx();
        self.path_and_query.extend(uri_part_encoder_iter(p, false));
        self.path_and_query.push(b'=');
        self.path_and_query.extend(uri_part_encoder_iter(v, false));
        self
    }
    /// add 64-bit int
    pub fn add_pi(mut self, p: &str, v: i64) -> QueryEncoder {
        self.pfx();
        self.path_and_query.extend(uri_part_encoder_iter(p, false));
        self.path_and_query.push(b'=');
        self.path_and_query.extend(format!("{}", v).bytes());
        self
    }
    /// add bool
    pub fn add_pb(mut self, p: &str, v: bool) -> QueryEncoder {
        self.pfx();
        self.path_and_query.extend(uri_part_encoder_iter(p, false));
        self.path_and_query.push(b'=');
        self.path_and_query.extend(format!("{}", if v { "true" } else { "false" }).bytes());
        self
    }
    /// add octal (hdfs permissions)
    pub fn add_po(mut self, p: &str, v: u16) -> QueryEncoder {
        self.pfx();
        self.path_and_query.extend(uri_part_encoder_iter(p, false));
        self.path_and_query.push(b'=');
        self.path_and_query.extend(format!("{}{}{}", (v & 0o0700) >> 6, (v & 0o0070) >> 3, (v & 0o0007)).bytes());
        self
    }    
    pub fn result(self) -> Vec<u8> { self.path_and_query }

    #[allow(dead_code)]
    pub fn result_with_fragment(mut self, f: &str) -> Vec<u8> {
        if !f.is_empty() {
            self.path_and_query.push(b'#');
            self.path_and_query.extend(uri_part_encoder_iter(f, false));
        }
        self.path_and_query 
    }
}

pub struct PathEncoder {
    path_and_query: Vec<u8>
}

impl PathEncoder {
    pub fn empty() -> Self { PathEncoder { path_and_query: vec![] } }
    pub fn extend(mut self, path_seg: &str) -> Self {
        match (self.path_and_query.last(), path_seg.bytes().next()) {
            (Some(b'/'), Some(b'/')) => { self.path_and_query.pop(); }
            (Some(c1), Some(c2)) if *c1 != b'/' && c2 != b'/' => { self.path_and_query.push(b'/'); }
            _ => ()
        }
        self.path_and_query.extend(uri_part_encoder_iter(path_seg, true));
        self
    }
    pub fn new(base_path: &str) -> Self { Self::empty().extend(base_path) }

    pub fn query(self) -> QueryEncoder {
        QueryEncoder { path_and_query: self.path_and_query, qm_added: false }
    }

    #[allow(dead_code)]
    pub fn result(self) -> Vec<u8> { self.path_and_query } 
}


#[test]
fn path_and_query_encoder_test() {
    let p0 = PathEncoder::new("/a/b/c/");
    let p1 = p0.extend("/d/e");
    let p2 = p1.extend("f/g");

    assert_eq!("/a/b/c/d/e/f/g".bytes().collect::<Vec<u8>>(), p2.path_and_query);

    let q0 = p2.query();
    assert_eq!("/a/b/c/d/e/f/g".bytes().collect::<Vec<u8>>(), q0.path_and_query);

    let q1 = q0.add_pv("пара/метр", "знач");
    assert_eq!("/a/b/c/d/e/f/g?%D0%BF%D0%B0%D1%80%D0%B0%2F%D0%BC%D0%B5%D1%82%D1%80=%D0%B7%D0%BD%D0%B0%D1%87".bytes().collect::<Vec<u8>>(), q1.path_and_query);    

    let q2 = q1.add_pi("g", 128);
    assert_eq!("/a/b/c/d/e/f/g?%D0%BF%D0%B0%D1%80%D0%B0%2F%D0%BC%D0%B5%D1%82%D1%80=%D0%B7%D0%BD%D0%B0%D1%87&g=128".bytes().collect::<Vec<u8>>(), q2.path_and_query);
}