from typing import Optional
from sqlalchemy import Integer, Text
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base


class AlmacenMaestro(Base):
    __tablename__ = "almacen_maestro"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nombre: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    puerto: Mapped[Optional[str]] = mapped_column(Text)
