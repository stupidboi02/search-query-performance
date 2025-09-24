from sqlalchemy import create_engine
from e import ExtractMonthly, ExtractQuarterly
from t import TransformMonthly, TransformQuarterly

### Config Database
username="postgres"
hostname="localhost"
port=5432
database="AmazonReport"
password="admin"
engine = create_engine(f'postgresql://{username}:{password}@{hostname}:{port}/{database}')
print("Connected to the PostgreSQL database successfully!")

def load(table:str, df):
    df.to_sql(name=table, con=engine, if_exists="replace", index=False)
    return

if __name__ == "__main__":
    df_month = ExtractMonthly('monthly').run
    df_month_clean = TransformMonthly(df_month).transform
    load('monthly_report',df_month_clean)

    df_quarter = ExtractQuarterly('quarterly').run
    df_quarter_clean = TransformQuarterly(df_quarter).transform
    load('quarterly_report',df_quarter_clean)


