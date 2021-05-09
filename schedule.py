import sqlalchemy as sqla
import pandas as pd
import datetime as dt

def send_to_pgadmin(table, schema, datapipe, dataframe, type_of_export="replace"):
    engine = sqla.create_engine(datapipe, executemany_mode="batch")
    dataframe.to_sql(
        table,
        engine,
        schema=schema,
        if_exists=type_of_export,
        chunksize=10000,
        index=False,
        method="multi",
    )


if __name__ == "__main__":
    data_pipe = "postgres://fjwcyikvradvjs:fb80c9c0af4ff3aa63946d0859b98e016be65aeea12dfe54b564ee36897509bc@ec2-52" \
                "-213-167-210.eu-west-1.compute.amazonaws.com:5432/dfse8ngisnkfa7"
    send_to_pgadmin("test", "public", data_pipe,
                    pd.DataFrame([{'date': dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}]), "append")
