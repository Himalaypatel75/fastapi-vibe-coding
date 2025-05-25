from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
import io

app = FastAPI()

# Database setup (using SQLite for simplicity)
DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Models
class Company(Base):
    __tablename__ = "companies"
    id = Column(Integer, primary_key=True, index=True)
    company_name = Column(String, unique=True, index=True)
    employees = relationship("Employee", back_populates="company")

class Employee(Base):
    __tablename__ = "employees"
    id = Column(Integer, primary_key=True, index=True)
    employee_id = Column(Integer, unique=True, index=True)
    first_name = Column(String)
    last_name = Column(String)
    phone_number = Column(String)
    salary = Column(Float)
    manager_id = Column(Integer, nullable=True)
    department_id = Column(Integer, nullable=True)
    company_id = Column(Integer, ForeignKey("companies.id"))
    company = relationship("Company", back_populates="employees")

Base.metadata.create_all(bind=engine)

# Pydantic Schemas
class EmployeeCreate(BaseModel):
    employee_id: int
    first_name: str
    last_name: str
    phone_number: str
    salary: float
    manager_id: Optional[int]
    department_id: Optional[int]
    company_name: str

class UploadResponse(BaseModel):
    message: str
    companies_created: int
    employees_created: int

class EmployeeResponse(BaseModel):
    employee_id: int
    first_name: str
    last_name: str
    phone_number: str
    salary: float
    manager_id: Optional[int]
    department_id: Optional[int]
    company_name: str

@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")

@app.post("/upload-employees/", response_model=UploadResponse)
async def upload_employees(file: UploadFile = File(...)):
    if not (file.filename.endswith('.xlsx') or file.filename.endswith('.csv')):
        raise HTTPException(status_code=400, detail="File must be .xlsx or .csv")
    try:
        content = await file.read()
        if file.filename.endswith('.xlsx'):
            df = pd.read_excel(io.BytesIO(content))
        else:
            df = pd.read_csv(io.StringIO(content.decode()))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")

    required_columns = [
        "EMPLOYEE_ID", "FIRST_NAME", "LAST_NAME", "PHONE_NUMBER",
        "COMPANY_NAME", "SALARY", "MANAGER_ID", "DEPARTMENT_ID"
    ]
    if not all(col in df.columns for col in required_columns):
        raise HTTPException(status_code=400, detail="Missing required columns in file.")

    # Remove duplicates and prepare company data
    company_names = df["COMPANY_NAME"].drop_duplicates().tolist()
    session = SessionLocal()
    try:
        # Bulk insert companies (ignore if already exists)
        existing_companies = session.query(Company).filter(Company.company_name.in_(company_names)).all()
        existing_company_names = {c.company_name for c in existing_companies}
        new_companies = [Company(company_name=name) for name in company_names if name not in existing_company_names]
        session.bulk_save_objects(new_companies)
        session.commit()

        # Get all companies with their ids
        all_companies = session.query(Company).filter(Company.company_name.in_(company_names)).all()
        company_name_to_id = {c.company_name: c.id for c in all_companies}

        # Prepare employee data for bulk insert
        employees_to_create = []
        for _, row in df.iterrows():
            employees_to_create.append(Employee(
                employee_id=int(row["EMPLOYEE_ID"]),
                first_name=str(row["FIRST_NAME"]),
                last_name=str(row["LAST_NAME"]),
                phone_number=str(row["PHONE_NUMBER"]),
                salary=float(row["SALARY"]),
                manager_id=int(row["MANAGER_ID"]) if not pd.isnull(row["MANAGER_ID"]) else None,
                department_id=int(row["DEPARTMENT_ID"]) if not pd.isnull(row["DEPARTMENT_ID"]) else None,
                company_id=company_name_to_id[row["COMPANY_NAME"]]
            ))
        session.bulk_save_objects(employees_to_create)
        session.commit()
        return UploadResponse(
            message="Data uploaded successfully.",
            companies_created=len(new_companies),
            employees_created=len(employees_to_create)
        )
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        session.close()

@app.get("/employees/", response_model=List[EmployeeResponse])
async def get_employees():
    session = SessionLocal()
    try:
        employees = session.query(Employee).all()
        result = []
        for emp in employees:
            company = session.query(Company).filter(Company.id == emp.company_id).first()
            result.append(EmployeeResponse(
                employee_id=emp.employee_id,
                first_name=emp.first_name,
                last_name=emp.last_name,
                phone_number=emp.phone_number,
                salary=emp.salary,
                manager_id=emp.manager_id,
                department_id=emp.department_id,
                company_name=company.company_name if company else None
            ))
        return result
    finally:
        session.close()

import uvicorn

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
