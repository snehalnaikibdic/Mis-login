import psycopg2
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Specify frontend domain
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

DB_HOST = ibdic-uat-postgresql.cueu6efldpx8.ap-south-1.rds.amazonaws.com
DB_NAME = invoice_sit_db
DB_USER = 'postgres'
DB_PASSWORD = $2A4b6C8d

conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

cur = conn.cursor()

class MISData(BaseModel):
    idpId: str

def get_user_mis_report(idpId: str):
    # Dynamic SQL query to retrieve data based on idpId
    cur.execute("""
        SELECT *
        FROM (
            SELECT *
            FROM information_schema.columns
            WHERE column_name = 'idpId'
        ) AS tables
        WHERE idpId = %s
    """, (idpId,))
    data = cur.fetchall()
    return data

@app.get("/user_mis_report")
def get_user_mis_report_endpoint():
    idpId = get_logged_in_user_idpId()  # assume this function returns the idpId of the logged in user
    cur.execute("SELECT * FROM Merchant_details WHERE idpId = %s", (idpId,))
    data = cur.fetchall()
    # remove the idpId column from the result
    data = [{k: v for k, v in row.items() if k != 'idpId'} for row in data]
    return {"data": data}