import io
import os
from datetime import datetime
from typing import Optional

import pandas as pd
from bson import ObjectId
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.exceptions import HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from pymongo import MongoClient

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
assert MONGODB_URI is not None, "MONGODB_URI is not set"


class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid objectid")
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")


class Sale(BaseModel):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    key: str = Field(...)
    distribution: str = Field(...)
    rep: str = Field(...)
    item: str = Field(...)
    sale: float = Field(...)
    quantity: int = Field(...)
    uom: str = Field(...)
    date: datetime = Field(...)
    customer: str = Field(...)
    ship_to_name: str = Field(...)
    addr1: str = Field(...)
    addr2: Optional[str] = Field(...)
    city: str = Field(...)
    state: str = Field(...)
    postal: str = Field(...)
    country: Optional[str] = Field(...)
    contract: Optional[str] = Field(...)
    cust_nbr: Optional[str] = Field(...)
    notes: Optional[dict[str, str]] = Field(...)
    gpo: Optional[str] = Field(...)
    rebate: Optional[float] = Field(...)
    net: Optional[float] = Field(...)

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example": {
                "_id": ObjectId("63c849031aeef6dd27ed3bac"),
                "key": "NDC-DECEMBER-2022",
                "distribution": "NDC",
                "rep": "Rep40",
                "item": "851",
                "sale": 68,
                "quantity": 1,
                "uom": "CS",
                "date": "12/22/2022",
                "customer": "Marc Trager",
                "ship_to_name": "Marc Trager",
                "addr1": "14273 Mulberry Dr",
                "addr2": "",
                "city": "Los Gatos",
                "state": "CA",
                "postal": "95032",
                "country": "",
                "contract": "SPECIAL PRICING",
                "cust_nbr": "",
                "notes": {"invoice": "3857526"},
                "gpo": "",
                "rebate": 0,
                "net": 68,
            }
        }


## initialize database
client = MongoClient(MONGODB_URI)
db = client["busse_sales_data_warehouse"]

sales = db["sales"]

app = FastAPI()

origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/sales")
async def get_sales(request: Request, format: str = "csv"):
    df = pd.DataFrame(list(sales.find()))

    df["net"] = df.apply(
        lambda x: x["sale"] if x["rebate"] == 0 else x["rebate"], axis=1
    )

    if format == "csv":
        stream = io.StringIO()
        df.to_csv(stream, index=False, encoding="utf-8", sep="|")
        # PlainTextResponse(stream.getvalue(), media_type="text/csv")
        return stream.getvalue()

    return [Sale(**sale) for sale in df.to_dict("records")]


@app.get("/api/sales/{month}/{year}")
def get_sales_for_period(month: str, year: str):
    months = [
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    ]

    years = ["2022", "2023"]

    if month.lower() not in months:
        raise HTTPException(status_code=404, detail="Month not found")

    if year not in years:
        raise HTTPException(status_code=404, detail="Year not found")

    df = pd.DataFrame(
        list(
            sales.find(
                {
                    "key": {
                        "$regex": f"{month}-{year}$",
                        "$options": "i",
                    }
                }
            )
        )
    )
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")

    response.headers["Content-Disposition"] = "attachment; filename=export.csv"
    return response


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8191, reload=True)
