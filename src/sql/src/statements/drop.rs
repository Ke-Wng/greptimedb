// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Display;

use sqlparser::ast::ObjectName;
use sqlparser_derive::{Visit, VisitMut};

/// DROP TABLE statement.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct DropTable {
    table_name: ObjectName,
    /// drop table if exists
    drop_if_exists: bool,
}

impl DropTable {
    /// Creates a statement for `DROP TABLE`
    pub fn new(table_name: ObjectName, if_exists: bool) -> Self {
        Self {
            table_name,
            drop_if_exists: if_exists,
        }
    }

    pub fn table_name(&self) -> &ObjectName {
        &self.table_name
    }

    pub fn drop_if_exists(&self) -> bool {
        self.drop_if_exists
    }
}

impl Display for DropTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DROP TABLE")?;
        if self.drop_if_exists() {
            f.write_str(" IF EXISTS")?;
        }
        let table_name = self.table_name();
        write!(f, r#" {table_name}"#)
    }
}

/// DROP DATABASE statement.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct DropDatabase {
    name: ObjectName,
    /// drop table if exists
    drop_if_exists: bool,
}

impl DropDatabase {
    /// Creates a statement for `DROP DATABASE`
    pub fn new(name: ObjectName, if_exists: bool) -> Self {
        Self {
            name,
            drop_if_exists: if_exists,
        }
    }

    pub fn name(&self) -> &ObjectName {
        &self.name
    }

    pub fn drop_if_exists(&self) -> bool {
        self.drop_if_exists
    }
}

impl Display for DropDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DROP DATABASE")?;
        if self.drop_if_exists() {
            f.write_str(" IF EXISTS")?;
        }
        let name = self.name();
        write!(f, r#" {name}"#)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::statement::Statement;

    #[test]
    fn test_display_drop_database() {
        let sql = r"drop database test;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::DropDatabase { .. });

        match &stmts[0] {
            Statement::DropDatabase(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
DROP DATABASE test"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = r"drop database if exists test;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::DropDatabase { .. });

        match &stmts[0] {
            Statement::DropDatabase(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
DROP DATABASE IF EXISTS test"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }

    #[test]
    fn test_display_drop_table() {
        let sql = r"drop table test;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::DropTable { .. });

        match &stmts[0] {
            Statement::DropTable(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
DROP TABLE test"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = r"drop table if exists test;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::DropTable { .. });

        match &stmts[0] {
            Statement::DropTable(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
DROP TABLE IF EXISTS test"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }
}
