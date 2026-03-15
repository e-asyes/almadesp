from datetime import datetime, date
from decimal import Decimal
from typing import Optional
from sqlalchemy import Integer, Text, Numeric, DateTime, Date, func
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base


class ManifiestoBL(Base):
    __tablename__ = "manifiesto_bl"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nro_manifiesto: Mapped[str] = mapped_column(Text, nullable=False)
    nave: Mapped[Optional[str]] = mapped_column(Text)
    sentido: Mapped[Optional[str]] = mapped_column(Text)
    fecha_arribo_zarpe: Mapped[Optional[datetime]] = mapped_column(DateTime)
    cia_naviera: Mapped[Optional[str]] = mapped_column(Text)
    fecha_emision_manifiesto: Mapped[Optional[date]] = mapped_column(Date)
    n_bl: Mapped[Optional[str]] = mapped_column(Text)
    almacen: Mapped[Optional[str]] = mapped_column(Text)
    fecha_aceptacion: Mapped[Optional[date]] = mapped_column(Date)
    puerto_desembarque: Mapped[Optional[str]] = mapped_column(Text)
    total_peso: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default=func.now())
    despacho: Mapped[Optional[str]] = mapped_column(Text)
    almacen_real: Mapped[Optional[str]] = mapped_column(Text)
    usuario_actualizacion: Mapped[Optional[str]] = mapped_column(Text)
    fecha_actualizacion_manual: Mapped[Optional[datetime]] = mapped_column(DateTime)
