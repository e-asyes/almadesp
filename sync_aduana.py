#!/usr/bin/env python3
"""
Standalone script to sync Aduana manifest data for upcoming maritime arrivals.

Queries siscon DB for despachos arriving in the next 7 days, consults Aduana
(isidora.aduana.cl) for each BL, and upserts results into Azure PostgreSQL.

Designed for cron:
  0 6 * * * cd /path/to/almadesp && python sync_aduana.py
"""

import asyncio
import logging
import os
import re
import ssl
import sys
import time
import warnings
from datetime import datetime, date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

# ── Suppress warnings ───────────────────────────────────────────────────────

warnings.filterwarnings("ignore")

# ── Load .env ────────────────────────────────────────────────────────────────

load_dotenv(Path(__file__).resolve().parent / ".env")

# ── Logging ──────────────────────────────────────────────────────────────────

LOG_FILE = Path(__file__).resolve().parent / "sync_aduana.log"

logger = logging.getLogger("sync_aduana")
logger.setLevel(logging.INFO)

_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

_ch = logging.StreamHandler(sys.stdout)
_ch.setFormatter(_fmt)
logger.addHandler(_ch)

_fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
_fh.setFormatter(_fmt)
logger.addHandler(_fh)

# ── Config ───────────────────────────────────────────────────────────────────

ADUANA_URL = "https://isidora.aduana.cl/WebManifiestoMaritimo/Consultas/CON_BlsxMFTO.jsp?Action=Event"

DAYS_AHEAD = 7


def _make_async_url(env_key: str, needs_ssl: bool = False) -> str:
    """Convert a postgresql:// URL from .env into a postgresql+asyncpg:// URL."""
    url = os.environ.get(env_key, "")
    if not url:
        logger.error("Missing env var: %s", env_key)
        sys.exit(1)
    url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    # asyncpg handles SSL via connect_args, not query params
    url = url.replace("?sslmode=require", "").replace("&sslmode=require", "")
    return url


# ── Database engines ─────────────────────────────────────────────────────────

_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE

azure_engine = create_async_engine(_make_async_url("AZURE_PG_URL"), pool_size=5, connect_args={"ssl": _ssl_ctx})
siscon_engine = create_async_engine(_make_async_url("SISCON_PG_URL"), pool_size=5)

AzureSession = async_sessionmaker(azure_engine, class_=AsyncSession, expire_on_commit=False)
SisconSession = async_sessionmaker(siscon_engine, class_=AsyncSession, expire_on_commit=False)

# ── Aduana parsing ───────────────────────────────────────────────────────────


def _cell_text(cell) -> str:
    return cell.get_text(strip=True) if cell else ""


def _find_label_value(soup, label: str) -> Optional[str]:
    for td in soup.find_all("td"):
        if label in td.get_text():
            nxt = td.find_next_sibling("td")
            if nxt:
                val = _cell_text(nxt)
                return val if val else None
    return None


def _is_bl_table(table) -> bool:
    for td in table.find_all("td", class_="SimpleObjectTableCellTitle", recursive=True):
        if "BL" in td.get_text():
            return True
    return False


def _parse_manifests(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    manifests: list[dict] = []
    tables = soup.find_all("table")
    current_header: Optional[dict] = None
    current_bls: list[dict] = []

    for table in tables:
        table_class = " ".join(table.get("class", []))
        txt = table.get_text()

        if "SimpleObjectTableCell" in table_class and "DATOS CONSIGNADOS" in txt:
            if current_header is not None:
                manifests.append({"header": current_header, "bls": current_bls})
                current_bls = []
            current_header = {
                "nro_manifiesto": _find_label_value(table, "Nro. Manifiesto"),
                "nave": _find_label_value(table, "Nave"),
                "sentido": _find_label_value(table, "Sentido"),
                "fecha_arribo_zarpe": _find_label_value(table, "Fecha Arribo/Zarpe"),
                "cia_naviera": _find_label_value(table, "Naviera"),
                "fecha_emision_manifiesto": _find_label_value(table, "Fecha Emisi"),
            }
        elif "SimpleObjectTable" in table_class and _is_bl_table(table):
            rows = table.find_all("tr")
            for row in rows[1:]:
                cells = row.find_all("td")
                if len(cells) >= 10:
                    current_bls.append({
                        "n_bl": _cell_text(cells[0]) or None,
                        "emisor": _cell_text(cells[1]) or None,
                        "fecha_emision": _cell_text(cells[2]) or None,
                        "fecha_aceptacion": _cell_text(cells[3]) or None,
                        "fecha_embarque": _cell_text(cells[4]) or None,
                        "almacen": _cell_text(cells[5]) or None,
                        "puerto_embarque": _cell_text(cells[6]) or None,
                        "puerto_desembarque": _cell_text(cells[7]) or None,
                        "ultimo_transbordo": _cell_text(cells[8]) or None,
                        "total_peso": _cell_text(cells[9]) or None,
                    })

    if current_header is not None:
        manifests.append({"header": current_header, "bls": current_bls})
    return manifests


# ── Aduana query ─────────────────────────────────────────────────────────────


async def query_aduana(bl: str) -> tuple[list[dict], Optional[str]]:
    form_data = {
        "EdNroGuia": bl,
        "EventSource": "cmdBuscar",
        "EventName": "buscarClick",
        "CON_ConsultaGralMFTOpageCode": "1",
        "totalManifiestos": "0",
    }
    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            resp = await client.post(ADUANA_URL, data=form_data)
            resp.raise_for_status()
    except httpx.TimeoutException:
        return [], "Timeout"
    except Exception as e:
        return [], str(e)

    html = resp.content.decode("iso-8859-1")
    try:
        manifests = _parse_manifests(html)
    except Exception:
        return [], "Parse error"

    if not manifests:
        return [], "Not found"
    return manifests, None


# ── BL splitting ─────────────────────────────────────────────────────────────


def split_bl(bl: str) -> list[str]:
    upper = bl.upper()
    if not re.findall(r'\([HN]\)', upper):
        return [bl]
    parts_raw = re.split(r'(\([HN]\))', upper)
    queries: list[str] = []
    for i, part in enumerate(parts_raw):
        part = part.strip()
        if not part or re.match(r'\([HN]\)$', part):
            continue
        if i > 0 and re.match(r'\([HN]\)$', parts_raw[i - 1]):
            queries.append(parts_raw[i - 1] + part)
        else:
            queries.append(part)
    return queries


# ── Date / decimal helpers ───────────────────────────────────────────────────


def parse_date(val: Optional[str]) -> Optional[date]:
    if not val:
        return None
    for fmt in ("%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(val.strip(), fmt).date()
        except ValueError:
            continue
    return None


def parse_datetime_val(val: Optional[str]) -> Optional[datetime]:
    if not val:
        return None
    for fmt in ("%d/%m/%Y %H:%M", "%d-%m-%Y %H:%M", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(val.strip(), fmt)
        except ValueError:
            continue
    return None


def parse_decimal(val: Optional[str]) -> Optional[Decimal]:
    if not val:
        return None
    try:
        return Decimal(val.strip())
    except (InvalidOperation, ValueError):
        return None


# ── DB upsert (raw SQL, no ORM imports) ──────────────────────────────────────


async def upsert_manifests(db: AsyncSession, despacho: str, manifests: list[dict]) -> tuple[int, int]:
    """Upsert manifest BL records. Returns (inserted, updated)."""
    inserted = 0
    updated = 0

    for manifest in manifests:
        header = manifest["header"]
        for bl in manifest["bls"]:
            n_bl = bl.get("n_bl")
            if not n_bl:
                continue

            new_almacen = bl.get("almacen")
            new_puerto = bl.get("puerto_desembarque")
            new_nave = header.get("nave")

            # Check if exists
            row = (await db.execute(
                text("SELECT id, almacen, puerto_desembarque, nave FROM manifiesto_bl WHERE n_bl = :n_bl"),
                {"n_bl": n_bl},
            )).first()

            if row:
                # Skip if nothing changed
                if row.almacen == new_almacen and row.puerto_desembarque == new_puerto and row.nave == new_nave:
                    continue
                await db.execute(text("""
                    UPDATE manifiesto_bl SET
                        almacen = :almacen,
                        puerto_desembarque = :puerto_desembarque,
                        nave = :nave,
                        despacho = :despacho,
                        nro_manifiesto = :nro_manifiesto,
                        sentido = :sentido,
                        fecha_arribo_zarpe = :fecha_arribo_zarpe,
                        cia_naviera = :cia_naviera,
                        fecha_emision_manifiesto = :fecha_emision_manifiesto,
                        fecha_aceptacion = :fecha_aceptacion,
                        total_peso = :total_peso,
                        updated_at = NOW()
                    WHERE n_bl = :n_bl
                """), {
                    "n_bl": n_bl,
                    "almacen": new_almacen,
                    "puerto_desembarque": new_puerto,
                    "nave": new_nave,
                    "despacho": despacho,
                    "nro_manifiesto": header.get("nro_manifiesto") or "",
                    "sentido": header.get("sentido"),
                    "fecha_arribo_zarpe": parse_datetime_val(header.get("fecha_arribo_zarpe")),
                    "cia_naviera": header.get("cia_naviera"),
                    "fecha_emision_manifiesto": parse_date(header.get("fecha_emision_manifiesto")),
                    "fecha_aceptacion": parse_date(bl.get("fecha_aceptacion")),
                    "total_peso": parse_decimal(bl.get("total_peso")),
                })
                updated += 1
            else:
                await db.execute(text("""
                    INSERT INTO manifiesto_bl
                        (despacho, nro_manifiesto, nave, sentido, fecha_arribo_zarpe,
                         cia_naviera, fecha_emision_manifiesto, n_bl, almacen,
                         fecha_aceptacion, puerto_desembarque, total_peso, updated_at)
                    VALUES
                        (:despacho, :nro_manifiesto, :nave, :sentido, :fecha_arribo_zarpe,
                         :cia_naviera, :fecha_emision_manifiesto, :n_bl, :almacen,
                         :fecha_aceptacion, :puerto_desembarque, :total_peso, NOW())
                """), {
                    "despacho": despacho,
                    "nro_manifiesto": header.get("nro_manifiesto") or "",
                    "nave": new_nave,
                    "sentido": header.get("sentido"),
                    "fecha_arribo_zarpe": parse_datetime_val(header.get("fecha_arribo_zarpe")),
                    "cia_naviera": header.get("cia_naviera"),
                    "fecha_emision_manifiesto": parse_date(header.get("fecha_emision_manifiesto")),
                    "n_bl": n_bl,
                    "almacen": new_almacen,
                    "fecha_aceptacion": parse_date(bl.get("fecha_aceptacion")),
                    "puerto_desembarque": new_puerto,
                    "total_peso": parse_decimal(bl.get("total_peso")),
                })
                inserted += 1

    return inserted, updated


# ── Main ─────────────────────────────────────────────────────────────────────


async def main():
    start = time.monotonic()
    today = date.today()
    fecha_desde = today
    fecha_hasta = today + timedelta(days=DAYS_AHEAD)

    logger.info("=" * 70)
    logger.info("SYNC ADUANA - Inicio")
    logger.info("Rango: %s a %s", fecha_desde, fecha_hasta)
    logger.info("=" * 70)

    # 1. Query siscon for maritime despachos with BL in date range
    logger.info("Consultando siscon por despachos maritimos...")
    async with SisconSession() as db:
        result = await db.execute(text("""
            SELECT despacho, numero_conocimiento, TRIM(pto_desembarque) as puerto,
                   fecha_arribo_estimado, nombre_vehiculo
            FROM declaracion.archimp
            WHERE fecha_arribo_estimado BETWEEN :fecha_desde AND :fecha_hasta
              AND TRIM(numero_conocimiento) <> ''
              AND TRIM(cod_via_transporte) = '01'
              AND (__deleted IS NULL OR __deleted = 'false')
            ORDER BY fecha_arribo_estimado, despacho
        """), {"fecha_desde": fecha_desde, "fecha_hasta": fecha_hasta})
        despachos = result.fetchall()

    if not despachos:
        logger.info("No se encontraron despachos para el rango.")
        return

    logger.info("Encontrados %d despachos con BL", len(despachos))

    # 2. Query Aduana for each despacho and upsert results
    total_found = 0
    total_not_found = 0
    total_inserted = 0
    total_updated = 0
    total_errors = 0

    async with AzureSession() as db:
        for i, row in enumerate(despachos, 1):
            despacho = row[0]
            bl_number = (row[1] or "").strip()
            puerto = row[2]
            eta = row[3]

            if not bl_number:
                total_not_found += 1
                continue

            queries = split_bl(bl_number)
            despacho_found = False

            for q in queries:
                manifests, error = await query_aduana(q)

                if error:
                    if error == "Not found":
                        logger.debug("[%d/%d] %s BL=%s -> no encontrado", i, len(despachos), despacho, q)
                    else:
                        logger.warning("[%d/%d] %s BL=%s -> error: %s", i, len(despachos), despacho, q, error)
                        total_errors += 1
                    continue

                despacho_found = True
                ins, upd = await upsert_manifests(db, despacho, manifests)
                total_inserted += ins
                total_updated += upd

                # Log details for each manifest found
                for m in manifests:
                    nave = m["header"].get("nave") or "-"
                    for bl_detail in m["bls"]:
                        almacen = bl_detail.get("almacen") or "-"
                        logger.info(
                            "[%d/%d] %s OK  Nave: %s  Almacen: %s  BL: %s",
                            i, len(despachos), despacho, nave, almacen, bl_detail.get("n_bl", "-"),
                        )

            if not despacho_found:
                total_not_found += 1
                logger.info("[%d/%d] %s NO ENCONTRADO  BL: %s", i, len(despachos), despacho, bl_number[:60])
            else:
                total_found += 1

        await db.commit()

    # 3. Summary
    elapsed = time.monotonic() - start
    logger.info("=" * 70)
    logger.info("SYNC ADUANA - Resumen")
    logger.info("  Total despachos:  %d", len(despachos))
    logger.info("  Encontrados:      %d", total_found)
    logger.info("  No encontrados:   %d", total_not_found)
    logger.info("  Insertados:       %d", total_inserted)
    logger.info("  Actualizados:     %d", total_updated)
    logger.info("  Errores:          %d", total_errors)
    logger.info("  Duracion:         %.1f s", elapsed)
    logger.info("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
