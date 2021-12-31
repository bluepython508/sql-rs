use std::net::{IpAddr, Ipv4Addr};

use sql::{Database, Ordering, Pool, Table};

#[allow(dead_code)]
#[derive(Debug, Table)]
#[sql(name = "12345")]
struct Test {
    id: i32,
    name: String,
    #[sql(name = "hello world", unique)]
    name_2: Option<String>,
    #[sql(name = "ABCD")]
    name_3: Option<i64>,
    #[sql(name = "123", unique)]
    r: f64,
    #[sql(as_str, name = "ip")]
    z: IpAddr,
}

async fn print(pool: &Pool<impl Database>) -> anyhow::Result<()> {
    let test = pool
        .select(Test::COLUMNS)
        .order_by(Test::id, Ordering::Ascending)
        .limit(500)
        .fetch_all::<Test>()
        .await?;
    println!("{:?}", test);
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let pool = Pool::connect("postgresql://postgres@localhost/").await.unwrap();
    // let pool = Pool::in_memory();
    pool.create::<Test>().if_not_exists().execute().await?;
    pool.insert_into(Test::COLUMNS)
        .values((
            23,
            "1234".to_string(),
            None,
            Some(12),
            1234.5,
            IpAddr::V4(Ipv4Addr::LOCALHOST),
        ))
        .execute()
        .await?;
    print(&pool).await?;
    pool.update()
        .set(Test::name_2, Some("1234".to_owned()))
        .set(Test::r, 12345.6)
        .r#where(Test::r.equals(1234.5))
        .execute()
        .await?;
    print(&pool).await?;
    pool.delete_where(Test::id.equals(23)).execute().await?;
    print(&pool).await?;
    Ok(())
}
