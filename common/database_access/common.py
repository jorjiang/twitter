import pandas as pd

from common.database_access.db_connection import DbConnection


def get_dataframe_from_db(database: str, query: str) -> pd.DataFrame:
    connection = DbConnection(database=database).connection
    df = pd.read_sql_query(query, connection.engine)

    return df


def get_dataframe_from_db_table(database: str, table_name: str):
    query = """
    select * from {}""".format(
        table_name
    )
    df = get_dataframe_from_db(database=database, query=query)
    return df


def truncate_table(database: str, table_name: str, check_foreign_key=True):
    truncate_with_foreign_key_checking_query = """
    truncate table {}.{}
    """.format(
        database,
        table_name
    )

    truncate_without_foreign_key_checking_query = """
    SET FOREIGN_KEY_CHECKS = 0; 
    truncate table {}.{};
    SET FOREIGN_KEY_CHECKS = 1;
    """.format(
        database,
        table_name
    )

    db = DbConnection(database=database).connection
    pdsql = pd.io.sql.SQLDatabase(db.engine)
    if check_foreign_key:
        pdsql.execute(truncate_with_foreign_key_checking_query)
    else:
        pdsql.execute(truncate_without_foreign_key_checking_query)
    return None


def update_table_from_dataframe(database: str, df: pd.DataFrame, table_name: str):
    db = DbConnection(database=database).connection
    df.to_sql(table_name, db.engine, if_exists="append", index=False)
    return None


def overwrite_ml_table_from_dataframe(
        df: pd.DataFrame, database: str, table_name: str, check_foreign_key: bool = True
):
    truncate_table(table_name=table_name, check_foreign_key=check_foreign_key)
    update_table_from_dataframe(df=df, database=database, table_name=table_name)
    return None
