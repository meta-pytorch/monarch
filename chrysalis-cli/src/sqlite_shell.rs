/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::ffi::CString;
use std::io::Write as _;

use anyhow::Context;
use anyhow::Result;
use libsql::Connection;
use libsql::Rows;
use libsql::Value;
use tokio::io::AsyncBufReadExt as _;
use tokio::io::BufReader;

pub(crate) async fn run(connection: &Connection) -> Result<()> {
    eprintln!("Chrysalis SQLite shell. Enter .help for help.");
    let mut input = BufReader::new(tokio::io::stdin());
    let mut sql = String::new();
    loop {
        print!(
            "{}",
            if sql.is_empty() {
                "sqlite> "
            } else {
                "   ...> "
            }
        );
        std::io::stdout().flush().context("flush SQLite prompt")?;
        let mut line = String::new();
        let length = tokio::select! {
            length = input.read_line(&mut line) => length.context("read SQLite input")?,
            result = tokio::signal::ctrl_c() => {
                result.context("wait for SQLite interrupt")?;
                println!();
                return Ok(());
            }
        };
        if length == 0 {
            println!();
            return Ok(());
        }

        if sql.is_empty() && is_meta_command(&line) {
            match run_meta_command(connection, line.trim()).await {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(error) => eprintln!("Error: {error:#}"),
            }
            continue;
        }

        sql.push_str(&line);
        match is_complete(&sql) {
            Ok(false) => continue,
            Ok(true) => {}
            Err(error) => {
                eprintln!("Error: {error:#}");
                sql.clear();
                continue;
            }
        }
        match execute(connection, &sql).await {
            Ok(output) => {
                print!("{output}");
                std::io::stdout().flush().context("flush SQLite output")?;
            }
            Err(error) => eprintln!("Error: {error:#}"),
        }
        sql.clear();
    }
}

pub(crate) async fn execute(connection: &Connection, sql: &str) -> Result<String> {
    let mut statements = connection.execute_batch(sql).await.context("execute SQL")?;
    let mut output = String::new();
    let mut result_index = 0;
    while let Some(result) = statements.next_stmt_row() {
        let Some(mut rows) = result else {
            continue;
        };
        append_rows(&mut output, &mut result_index, &mut rows).await?;
    }
    Ok(output)
}

fn is_meta_command(line: &str) -> bool {
    line.trim_start()
        .strip_prefix('.')
        .and_then(|command| command.chars().next())
        .is_some_and(|character| character.is_ascii_alphabetic())
}

async fn run_meta_command(connection: &Connection, command: &str) -> Result<bool> {
    let (name, argument) = command
        .split_once(char::is_whitespace)
        .map_or((command, ""), |(name, argument)| (name, argument.trim()));
    match name {
        ".exit" | ".quit" => return Ok(true),
        ".help" => {
            println!(".tables              List tables and views");
            println!(".schema [TABLE]      Show schema");
            println!(".exit                Exit the shell");
            println!(".quit                Exit the shell");
        }
        ".tables" if argument.is_empty() => {
            print_meta_query(
                connection,
                "SELECT name FROM sqlite_schema \
                 WHERE type IN ('table', 'view') \
                   AND name NOT LIKE 'sqlite_%' \
                 ORDER BY name",
                Vec::new(),
            )
            .await?;
        }
        ".tables" => eprintln!("Error: .tables does not accept arguments"),
        ".schema" => {
            let (filter, parameters) = if argument.is_empty() {
                ("", Vec::new())
            } else {
                ("AND tbl_name = ?1", vec![Value::Text(argument.to_owned())])
            };
            print_meta_query(
                connection,
                &format!(
                    "SELECT sql FROM sqlite_schema \
                     WHERE sql IS NOT NULL \
                       AND name NOT LIKE 'sqlite_%' \
                       {filter} \
                     ORDER BY type, name"
                ),
                parameters,
            )
            .await?;
        }
        _ => eprintln!("Error: unknown command {name:?}"),
    }
    Ok(false)
}

async fn print_meta_query(
    connection: &Connection,
    sql: &str,
    parameters: Vec<Value>,
) -> Result<()> {
    let mut rows = connection
        .query(sql, parameters)
        .await
        .context("execute SQLite meta-command")?;
    let mut output = String::new();
    let mut result_index = 0;
    append_rows(&mut output, &mut result_index, &mut rows).await?;
    print!("{output}");
    std::io::stdout()
        .flush()
        .context("flush SQLite meta-command output")?;
    Ok(())
}

async fn append_rows(output: &mut String, result_index: &mut usize, rows: &mut Rows) -> Result<()> {
    if *result_index > 0 {
        output.push('\n');
    }
    *result_index += 1;
    let columns = rows.column_count();
    for column in 0..columns {
        if column > 0 {
            output.push('\t');
        }
        output.push_str(rows.column_name(column).unwrap_or("?"));
    }
    output.push('\n');
    while let Some(row) = rows.next().await? {
        for column in 0..columns {
            if column > 0 {
                output.push('\t');
            }
            output.push_str(&format_value(row.get_value(column)?));
        }
        output.push('\n');
    }
    Ok(())
}

fn is_complete(sql: &str) -> Result<bool> {
    let sql = CString::new(sql).context("SQL contains a NUL byte")?;
    // SAFETY: `sql` is NUL-terminated and remains live for the duration of the call.
    Ok(unsafe { libsql_sys::ffi::sqlite3_complete(sql.as_ptr()) != 0 })
}

fn format_value(value: Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Integer(value) => value.to_string(),
        Value::Real(value) => value.to_string(),
        Value::Text(value) => value
            .replace('\\', "\\\\")
            .replace('\t', "\\t")
            .replace('\r', "\\r")
            .replace('\n', "\\n"),
        Value::Blob(value) => format!("x'{}'", format_bytes(&value)),
    }
}

fn format_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to a string cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use libsql::Builder;

    use super::*;

    #[test]
    fn statement_completion_uses_sqlite_tokenization() {
        assert!(!is_complete("SELECT ';'").unwrap());
        assert!(is_complete("SELECT ';';").unwrap());
        assert!(!is_complete("CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 1;").unwrap());
        assert!(is_complete("CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 1; END;").unwrap());
    }

    #[test]
    fn fractional_sql_is_not_a_meta_command() {
        assert!(!is_meta_command("  .5 + 1;"));
        assert!(is_meta_command("  .schema items"));
    }

    #[test]
    fn values_use_stable_tabular_rendering() {
        assert_eq!(format_value(Value::Null), "NULL");
        assert_eq!(format_value(Value::Integer(42)), "42");
        assert_eq!(
            format_value(Value::Text("one\ttwo\nthree\\four".into())),
            "one\\ttwo\\nthree\\\\four"
        );
        assert_eq!(format_value(Value::Blob(vec![0x00, 0xab])), "x'00ab'");
    }

    #[tokio::test]
    async fn execution_formats_multiple_result_sets() {
        let database = Builder::new_local(":memory:").build().await.unwrap();
        let connection = database.connect().unwrap();
        assert_eq!(
            execute(&connection, "SELECT 1 AS one; SELECT X'42' AS bytes;")
                .await
                .unwrap(),
            "one\n1\n\nbytes\nx'42'\n"
        );
    }
}
