pub mod catalog;
pub mod datasource;
pub mod error;
pub mod logical_plain;
pub mod parser;
pub mod planner;
pub mod udf;

#[cfg(test)]
mod tests {
    use sqlparser::dialect::Dialect;
    use sqlparser::parser::Parser;

    #[derive(Debug, Default)]
    pub struct RlinkDialect {}

    impl Dialect for RlinkDialect {
        fn is_delimited_identifier_start(&self, ch: char) -> bool {
            (ch == '"') || (ch == '\'') || (ch == '`')
        }

        fn is_identifier_start(&self, ch: char) -> bool {
            ('a'..='z').contains(&ch)
                || ('A'..='Z').contains(&ch)
                || ch == '_'
                || ch == '#'
                || ch == '@'
        }

        fn is_identifier_part(&self, ch: char) -> bool {
            ('a'..='z').contains(&ch)
                || ('A'..='Z').contains(&ch)
                || ('0'..='9').contains(&ch)
                || ch == '@'
                || ch == '$'
                || ch == '#'
                || ch == '_'
        }
    }

    #[test]
    pub fn sql_parser_test() {
        let sql = r#"
create table table_1(a varchar(50), b varchar(50))
    with (
    'connector' = 'kafka',
    'topic' = 'a',
    'broker-servers' = '192.168.1.1:9200',
    'startup-mode' = 'earliest-offset',
    'decode-mode' = 'java|json',
    'decode-java-class' = 'x.b.C'
);

SELECT TUMBLE_START(t, INTERVAL '1' minute) as wStart,
       TUMBLE_END(t, INTERVAL '1' minute) as wEnd,
       a, b, myfunc(b), 
       count(*)
FROM table_1 
WHERE a > b AND b < 100 
GROUP BY TUMBLE(t, INTERVAL '1' minute), a, b"#;

        //     let sql = "\
        //     create table table_1(a varchar(50), b varchar(50))\
        //     (
        //     'connector' = 'kafka'
        //     'topic' = 'a'
        //     'broker-servers' = '192.168.1.1:9200'
        //     'startup_mode' = 'earliest-offset',
        //     'decode_mode' = 'java|json',
        //     'decode-java-class' = 'x.b.C',
        // )";

        let dialect = RlinkDialect {}; // or AnsiDialect, or your own dialect ...

        let ast = Parser::parse_sql(&dialect, sql).unwrap();

        ast.iter().for_each(|s| {
            println!("{:?}", s);
        });
        println!("AST: {}", serde_json::to_string(&ast).unwrap());
    }
}
