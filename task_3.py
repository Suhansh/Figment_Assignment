import pandas as pd

def value_formatting(value):
    if pd.isna(value):
            return 'NULL'
    elif isinstance(value, str):
        if ',' in value: 
            try:
                formatted_value = float(value.replace(',', ''))
                return str(formatted_value)
            except ValueError:
                pass
        return "'{}'".format(value.replace("'", "''"))
    return str(value)

def sql_generator(csv_path: str, output_sql_path: str):
    try:
        rewards_df = pd.read_csv(csv_path)

        columns = rewards_df.columns
        sql_types = ["TEXT", "TEXT", "REAL", "REAL"]

        table = 'Rewards'
        SQL_STATEMENT = "CREATE TABLE {} (\n".format(table)
        for col, sql_type in zip(columns, sql_types):
            SQL_STATEMENT += "\t{} {},\n".format('"{}"'.format(col), sql_type)
        SQL_STATEMENT = SQL_STATEMENT.rstrip(',\n') + "\n);"

        statements = []
        quoted_columns = ['"{}"'.format(col) for col in columns]
        for _, row in rewards_df.iterrows():
            try:
                values = ', '.join(value_formatting(val) for val in row)
                insert = "INSERT INTO {} ({}) VALUES ({});" \
                    .format(table, ', '.join(quoted_columns), values)
                statements.append(insert)
            except Exception as e:
                print("Error generating INSERT statement for a row: {}".format(str(e)))
                continue
        
        with open(output_sql_path, 'w') as f:
            f.write(SQL_STATEMENT + '\n\n')
            for insert_sql in statements:
                f.write(insert_sql + '\n')
        
        print(f"SQL statements written to {output_sql_path}")
        
    except Exception as e:
        print(f"Error generating SQL queries")
        return

sql_generator(
    csv_path="./data/Rewards(Feb 1 - March 31).csv",
    output_sql_path="./data/out/rewards.sql"
)
