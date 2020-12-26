use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer, TokenizerError};

#[derive(Debug, Default)]
pub struct StreamingDialect;

impl Dialect for StreamingDialect {
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        let b = ch == '"';
        println!("is_delimited_identifier_start=>{}, {}", ch, b);
        b
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        let b = (ch >= 'a' && ch <= 'z')
            || (ch >= 'A' && ch <= 'Z')
            || ch == '_'
            || ch == '#'
            || ch == '@';
        // || ch == '\'';
        println!("is_identifier_start=>{}, {}", ch, b);
        b
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        let b = (ch >= 'a' && ch <= 'z')
            || (ch >= 'A' && ch <= 'Z')
            || (ch >= '0' && ch <= '9')
            || ch == '@'
            || ch == '$'
            || ch == '#'
            || ch == '_';
        println!("is_identifier_part=>{}, {}", ch, b);
        b
    }
}

pub fn parse() {
    let sql = "\
    create table table_1(a varchar(50), b varchar(50))\
    with (
    'connector' = 'kafka',
    'topic' = 'a',
    'broker-servers' = '192.168.1.1:9200'
    'startup-mode' = 'earliest-offset',
    'decode-mode' = 'java|json',
    'decode-java-class' = 'x.b.C',
);\
    \
    SELECT a, b, 123, 'str' as str, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";

    // let sql = "\
    // SELECT a, b, 123, myfunc(b) \
    //        FROM table_1 \
    //        WHERE a > b AND b < 100 \
    //        ORDER BY a DESC, b";
    let dialect = StreamingDialect {}; // or AnsiDialect, or your own dialect ...

    let tokens = parse_sql(&dialect, sql).unwrap();
    for token in tokens {
        println!("{}", print_token(&token))
    }

    let ast = Parser::parse_sql(&dialect, sql).unwrap();

    println!("AST: {:?}", ast);
}

pub fn parse_sql(dialect: &dyn Dialect, sql: &str) -> Result<Vec<Token>, TokenizerError> {
    let mut tokenizer = Tokenizer::new(dialect, &sql);
    tokenizer.tokenize()
}

fn print_token(token: &Token) -> String {
    match token {
        // An end-of-file marker, not a real token
        Token::EOF => "EOF".to_string(),
        // A keyword (like SELECT) or an optionally quoted SQL identifier
        Token::Word(word) => format!("Word({})", word),
        // An unsigned numeric literal
        Token::Number(string) => format!("Number({})", string),
        // A character that could not be tokenized
        Token::Char(char) => format!("Char({})", char),
        // Single quoted string: i.e: 'string'
        Token::SingleQuotedString(string) => format!("SingleQuotedString({})", string),
        // "National" string literal: i.e: N'string'
        Token::NationalStringLiteral(string) => format!("NationalStringLiteral({})", string),
        // Hexadecimal string literal: i.e.: X'deadbeef'
        Token::HexStringLiteral(string) => format!("HexStringLiteral({})", string),
        // Comma
        Token::Comma => "Comma".to_string(),
        // Whitespace (space, tab, etc)
        Token::Whitespace(whitespace) => format!("Whitespace({})", whitespace),
        // Equality operator `=`
        Token::Eq => "Eq(=)".to_string(),
        // Not Equals operator `<>` (or `!=` in some dialects)
        Token::Neq => "Neq(<>)".to_string(),
        // Less Than operator `<`
        Token::Lt => "Lt".to_string(),
        // Greater han operator `>`
        Token::Gt => "Gt".to_string(),
        // Less Than Or Equals operator `<=`
        Token::LtEq => "LtEq".to_string(),
        // Greater Than Or Equals operator `>=`
        Token::GtEq => "GtEq".to_string(),
        // Plus operator `+`
        Token::Plus => "Plus".to_string(),
        // Minus operator `-`
        Token::Minus => "Minus".to_string(),
        // Multiplication operator `*`
        Token::Mult => "Mult".to_string(),
        // Division operator `/`
        Token::Div => "Div".to_string(),
        // Modulo Operator `%`
        Token::Mod => "Mod".to_string(),
        // String concatenation `||`
        Token::StringConcat => "StringConcat".to_string(),
        // Left parenthesis `(`
        Token::LParen => "LParen".to_string(),
        // Right parenthesis `)`
        Token::RParen => "RParen".to_string(),
        // Period (used for compound identifiers or projections into nested types)
        Token::Period => "Period".to_string(),
        // Colon `:`
        Token::Colon => "Colon".to_string(),
        // DoubleColon `::` (used for casting in postgresql)
        Token::DoubleColon => "DoubleColon".to_string(),
        // SemiColon `;` used as separator for COPY and payload
        Token::SemiColon => "SemiColon".to_string(),
        // Backslash `\` used in terminating the COPY payload with `\.`
        Token::Backslash => "Backslash".to_string(),
        // Left bracket `[`
        Token::LBracket => "LBracket".to_string(),
        // Right bracket `]`
        Token::RBracket => "RBracket".to_string(),
        // Ampersand `&`
        Token::Ampersand => "Ampersand".to_string(),
        // Pipe `|`
        Token::Pipe => "Pipe".to_string(),
        // Caret `^`
        Token::Caret => "Caret".to_string(),
        // Left brace `{`
        Token::LBrace => "LBrace".to_string(),
        // Right brace `}`
        Token::RBrace => "RBrace".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use crate::parse;

    #[test]
    pub fn parse_sql_test() {
        parse();
    }
}
