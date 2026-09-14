use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use sqlx::AssertSqlSafe;
use sqlx::SqlSafeStr;
use sqlx::migrate::MigrateError;
use sqlx::migrate::Migration;
use sqlx::migrate::MigrationType;
use sqlx::migrate::Migrator;
use std::borrow::Cow;

const INITIAL_SQL: &str =
    "CREATE TABLE fixture (value TEXT);\nINSERT INTO fixture VALUES ('preserved');\n";

fn fixture_migrator(sql: String, include_pending: bool) -> Migrator {
    let mut migrations = vec![Migration::new(
        1,
        Cow::Borrowed("initial"),
        MigrationType::Simple,
        AssertSqlSafe(sql).into_sql_str(),
        false,
    )];
    if include_pending {
        migrations.push(Migration::new(
            2,
            Cow::Borrowed("pending"),
            MigrationType::Simple,
            "CREATE TABLE pending (value TEXT);\n".into_sql_str(),
            false,
        ));
    }
    Migrator {
        migrations: Cow::Owned(migrations),
        ..super::runtime_goals_migrator()
    }
}

#[tokio::test]
async fn runtime_migrations_support_spawned_tasks() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    tokio::spawn(async move {
        let migrator = fixture_migrator(INITIAL_SQL.to_owned(), /*include_pending*/ false);
        super::run_runtime_migrations(&pool, &migrator).await
    })
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test]
async fn line_endings_preserve_checksums_and_data_in_both_directions() {
    for (stored, embedded) in [
        (INITIAL_SQL.replace('\n', "\r\n"), INITIAL_SQL.to_owned()),
        (INITIAL_SQL.to_owned(), INITIAL_SQL.replace('\n', "\r\n")),
    ] {
        let home = crate::runtime::test_support::unique_temp_dir();
        tokio::fs::create_dir_all(&home).await.unwrap();
        let _cleanup = scopeguard::guard(home.clone(), |home| {
            let _ = std::fs::remove_dir_all(home);
        });
        let sqlite = crate::SqliteConfig::new_for_testing(home.as_path().abs());
        let original = fixture_migrator(stored, /*include_pending*/ false);
        let pool = sqlite.open_goals_db(&original, None).await.unwrap();
        pool.close().await;

        let current = fixture_migrator(embedded, /*include_pending*/ true);
        let pool = sqlite.open_goals_db(&current, None).await.unwrap();
        let checksum: Vec<u8> =
            sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations WHERE version = 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(checksum, original.migrations[0].checksum.as_ref());
        let value: String = sqlx::query_scalar("SELECT value FROM fixture")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(value, "preserved");
        let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM pending")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(pending, 0);
        // Reopening with the original binary must still validate its checksum.
        original.run(&pool).await.unwrap();
        pool.close().await;
    }
}

#[tokio::test]
async fn line_endings_do_not_accept_changed_sql_or_dirty_migrations() {
    let home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&home).await.unwrap();
    let _cleanup = scopeguard::guard(home.clone(), |home| {
        let _ = std::fs::remove_dir_all(home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(home.as_path().abs());
    let original = fixture_migrator(INITIAL_SQL.to_owned(), /*include_pending*/ false);
    let pool = sqlite.open_goals_db(&original, None).await.unwrap();
    let changed = fixture_migrator(
        INITIAL_SQL.replace("preserved", "changed"),
        /*include_pending*/ false,
    );
    let error = sqlite.open_goals_db(&changed, None).await.unwrap_err();
    assert!(error.chain().any(|error| matches!(
        error.downcast_ref::<MigrateError>(),
        Some(MigrateError::VersionMismatch(1))
    )));

    sqlx::query("UPDATE _sqlx_migrations SET success = 0 WHERE version = 1")
        .execute(&pool)
        .await
        .unwrap();
    let alternate = fixture_migrator(
        INITIAL_SQL.replace('\n', "\r\n"),
        /*include_pending*/ false,
    );
    let error = sqlite.open_goals_db(&alternate, None).await.unwrap_err();
    assert!(error.chain().any(|error| matches!(
        error.downcast_ref::<MigrateError>(),
        Some(MigrateError::Dirty(1))
    )));
    pool.close().await;
}
