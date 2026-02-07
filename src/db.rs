use sqlx::{
    migrate::MigrateDatabase,
    sqlite::{Sqlite, SqlitePool},
    Pool,
};

pub type DbPool = Pool<Sqlite>;

pub async fn ensure_database_file(url: &str) -> Result<(), sqlx::Error> {
    let exists = Sqlite::database_exists(url).await?;

    if !exists {
        Sqlite::create_database(url).await?;
    }

    Ok(())
}

pub async fn init_pool(database_url: &str) -> Result<DbPool, sqlx::Error> {
    ensure_database_file(database_url).await?;
    let pool = SqlitePool::connect(database_url).await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS records (
            ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT NOT NULL,
            value BLOB,
            timestamp INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000)
        )
        "#,
    )
    .execute(&pool)
    .await?;

    Ok(pool)
}
