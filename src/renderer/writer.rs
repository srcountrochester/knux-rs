use super::config::PlaceholderStyle;

pub struct SqlWriter {
    pub buf: String,
    pub next_param_idx: usize, // 1-based для $1/$2..., игнорится при '?'
    pub placeholders: PlaceholderStyle,
}

impl SqlWriter {
    pub fn new(cap: usize, placeholders: PlaceholderStyle) -> Self {
        Self {
            buf: String::with_capacity(cap),
            next_param_idx: 1,
            placeholders,
        }
    }

    #[inline]
    pub fn push<S: AsRef<str>>(&mut self, s: S) {
        self.buf.push_str(s.as_ref());
    }

    #[inline]
    pub fn push_char(&mut self, c: char) {
        self.buf.push(c);
    }

    /// Вставляет плейсхолдер (увеличивая счётчик при Numbered)
    pub fn push_placeholder(&mut self) {
        match self.placeholders {
            PlaceholderStyle::Question => self.push("?"),
            PlaceholderStyle::Numbered => {
                let i = self.next_param_idx;
                self.next_param_idx += 1;
                // $1, $2...
                self.buf.push('$');
                self.buf.push_str(&i.to_string());
            }
        }
    }

    pub fn finish(self) -> String {
        self.buf
    }

    #[inline]
    pub fn push_u64(&mut self, v: u64) {
        use itoa::Buffer;
        let mut buf = Buffer::new();
        self.buf.push_str(buf.format(v));
    }

    #[inline]
    pub fn push_sep(&mut self, i: usize, sep: &str) {
        if i > 0 {
            self.buf.push_str(sep);
        }
    }
}
