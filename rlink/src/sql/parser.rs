use std::collections::HashMap;

use sqlparser::ast::{ColumnDef, ColumnOptionDef, TableConstraint, Value};
use sqlparser::ast::{SqlOption, Statement as SQLStatement};
use sqlparser::dialect::Dialect;
use sqlparser::dialect::GenericDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, Tokenizer};

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

/// rlink extension DDL for `CREATE EXTERNAL TABLE`
#[derive(Debug, Clone, PartialEq)]
pub struct CreateExternalTable {
    /// Table name
    pub name: String,
    /// Optional schema
    pub columns: Vec<ColumnDef>,
    /// Table's properties
    pub with_options: HashMap<String, Value>,
}

/// rlink Statement representations.
///
/// Tokens parsed by `DFParser` are converted into these values.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// ANSI SQL AST node
    Statement(Box<SQLStatement>),
    /// Extension: `CREATE EXTERNAL TABLE`
    CreateExternalTable(CreateExternalTable),
}

/// SQL Parser
pub struct DFParser<'a> {
    parser: Parser<'a>,
}

impl<'a> DFParser<'a> {
    /// Parse the specified tokens
    pub fn new(sql: &str) -> Result<Self, ParserError> {
        let dialect = &GenericDialect {};
        DFParser::new_with_dialect(sql, dialect)
    }

    /// Parse the specified tokens with dialect
    pub fn new_with_dialect(sql: &str, dialect: &'a dyn Dialect) -> Result<Self, ParserError> {
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;

        Ok(DFParser {
            parser: Parser::new(tokens, dialect),
        })
    }

    /// Parse a SQL statement and produce a set of statements with dialect
    pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParserError> {
        let dialect = &GenericDialect {};
        DFParser::parse_sql_with_dialect(sql, dialect)
    }

    /// Parse a SQL statement and produce a set of statements
    pub fn parse_sql_with_dialect(
        sql: &str,
        dialect: &dyn Dialect,
    ) -> Result<Vec<Statement>, ParserError> {
        let mut parser = DFParser::new_with_dialect(sql, dialect)?;
        let mut stmts = Vec::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser.expected("end of statement", parser.parser.peek_token());
            }

            let statement = parser.parse_statement()?;
            stmts.push(statement);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }

    /// Report unexpected token
    fn expected<T>(&self, expected: &str, found: Token) -> Result<T, ParserError> {
        parser_err!(format!("Expected {}, found: {}", expected, found))
    }

    /// Parse a new expression
    pub fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        match self.parser.peek_token() {
            Token::Word(w) => {
                match w.keyword {
                    Keyword::CREATE => {
                        // move one token forward
                        self.parser.next_token();
                        // use custom parsing
                        self.parse_create()
                    }
                    _ => {
                        // use the native parser
                        Ok(Statement::Statement(Box::from(
                            self.parser.parse_statement()?,
                        )))
                    }
                }
            }
            _ => {
                // use the native parser
                Ok(Statement::Statement(Box::from(
                    self.parser.parse_statement()?,
                )))
            }
        }
    }

    /// Parse a SQL CREATE statement
    pub fn parse_create(&mut self) -> Result<Statement, ParserError> {
        if self.parser.parse_keyword(Keyword::EXTERNAL) {
            self.parse_create_external_table()
        } else {
            Ok(Statement::Statement(Box::from(self.parser.parse_create()?)))
        }
    }

    // This is a copy of the equivalent implementation in sqlparser.
    fn parse_columns(&mut self) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>), ParserError> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.parser.consume_token(&Token::LParen) || self.parser.consume_token(&Token::RParen) {
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parser.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Token::Word(_) = self.parser.peek_token() {
                let column_def = self.parse_column_def()?;
                columns.push(column_def);
            } else {
                return self.expected(
                    "column name or constraint definition",
                    self.parser.peek_token(),
                );
            }
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after column definition",
                    self.parser.peek_token(),
                );
            }
        }

        Ok((columns, constraints))
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef, ParserError> {
        let name = self.parser.parse_identifier()?;
        let data_type = self.parser.parse_data_type()?;
        let collation = if self.parser.parse_keyword(Keyword::COLLATE) {
            Some(self.parser.parse_object_name()?)
        } else {
            None
        };
        let mut options = vec![];
        loop {
            if self.parser.parse_keyword(Keyword::CONSTRAINT) {
                let name = Some(self.parser.parse_identifier()?);
                if let Some(option) = self.parser.parse_optional_column_option()? {
                    options.push(ColumnOptionDef { name, option });
                } else {
                    return self.expected(
                        "constraint details after CONSTRAINT <name>",
                        self.parser.peek_token(),
                    );
                }
            } else if let Some(option) = self.parser.parse_optional_column_option()? {
                options.push(ColumnOptionDef { name: None, option });
            } else {
                break;
            };
        }
        Ok(ColumnDef {
            name,
            data_type,
            collation,
            options,
        })
    }

    fn parse_create_external_table(&mut self) -> Result<Statement, ParserError> {
        self.parser.expect_keyword(Keyword::TABLE)?;
        let table_name = self.parser.parse_object_name()?;
        let (columns, _) = self.parse_columns()?;
        let with_options = self.parse_options()?;

        let create = CreateExternalTable {
            name: table_name.to_string(),
            columns,
            with_options,
        };
        Ok(Statement::CreateExternalTable(create))
    }

    /// Parses the set of valid formats
    fn parse_options(&mut self) -> Result<HashMap<String, Value>, ParserError> {
        let mut with_options = HashMap::new();

        let options: Vec<SqlOption> = self.parser.parse_options(Keyword::WITH)?;
        options.iter().for_each(|o: &SqlOption| {
            with_options.insert(o.name.value.clone(), o.value.clone());
        });

        Ok(with_options)
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{DataType, Ident};

    use super::*;

    fn expect_parse_ok(sql: &str, expected: Statement) -> Result<(), ParserError> {
        let statements = DFParser::parse_sql(sql)?;
        assert_eq!(
            statements.len(),
            1,
            "Expected to parse exactly one statement"
        );
        assert_eq!(statements[0], expected);
        Ok(())
    }

    fn make_column_def(name: impl Into<String>, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: Ident {
                value: name.into(),
                quote_style: None,
            },
            data_type,
            collation: None,
            options: vec![],
        }
    }

    #[test]
    fn create_external_table() -> Result<(), ParserError> {
        // positive case
        let sql = r#"
CREATE EXTERNAL TABLE table_1(a varchar(50), b varchar(50))
    WITH (
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
        let display = None;
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: "t".into(),
            columns: vec![make_column_def("c1", DataType::Int(display))],
            with_options: HashMap::new(),
        });
        expect_parse_ok(sql, expected)?;

        Ok(())
    }
}
