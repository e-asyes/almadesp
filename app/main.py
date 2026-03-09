import logging

from fastapi import FastAPI

from app.routers import almacen

logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="API Almacen Despacho",
    description="Consulta de almacen desde Aduana",
    version="1.0.0",
)

app.include_router(almacen.router)


@app.get("/")
async def root():
    return {"message": "API Almacen Despacho"}


@app.get("/health")
async def health_check():
    return {"status": "healthy"}
