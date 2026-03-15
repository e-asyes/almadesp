#!/usr/bin/env python3
"""Interactive CLI to update almacen data from Aduana for vessel arrivals."""

import asyncio
import ssl
import re
import sys
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

# ── Config ──────────────────────────────────────────────────────────────────

AZURE_PG_URL = "postgresql+asyncpg://rootpgres:PoRKmK8f7fBQKU@cexpgres.postgres.database.azure.com:5432/postgres"
SISCON_PG_URL = "postgresql+asyncpg://aleon:aleon856%25@64.176.3.224:57978/api-clientes"
ADUANA_URL = "https://isidora.aduana.cl/WebManifiestoMaritimo/Consultas/CON_BlsxMFTO.jsp?Action=Event"

# ── Database setup ──────────────────────────────────────────────────────────

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

azure_engine = create_async_engine(AZURE_PG_URL, pool_size=5, connect_args={"ssl": ssl_context})
siscon_engine = create_async_engine(SISCON_PG_URL, pool_size=5)

AzureSession = async_sessionmaker(azure_engine, class_=AsyncSession, expire_on_commit=False)
SisconSession = async_sessionmaker(siscon_engine, class_=AsyncSession, expire_on_commit=False)

# ── Colors ──────────────────────────────────────────────────────────────────

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

# ── Aduana parsing (same logic as router) ───────────────────────────────────

def _cell_text(cell) -> str:
    return cell.get_text(strip=True) if cell else ""


def _find_label_value(soup, label):
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
    manifests = []
    tables = soup.find_all("table")
    current_header = None
    current_bls = []

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


def split_bl(bl: str) -> list[str]:
    upper = bl.upper()
    if not re.findall(r'\([HN]\)', upper):
        return [bl]
    parts_raw = re.split(r'(\([HN]\))', upper)
    queries = []
    for i, part in enumerate(parts_raw):
        part = part.strip()
        if not part or re.match(r'\([HN]\)$', part):
            continue
        if i > 0 and re.match(r'\([HN]\)$', parts_raw[i - 1]):
            queries.append(parts_raw[i - 1] + part)
        else:
            queries.append(part)
    return queries


# ── Date/Decimal helpers ────────────────────────────────────────────────────

def parse_date(val):
    if not val:
        return None
    for fmt in ("%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(val.strip(), fmt).date()
        except ValueError:
            continue
    return None


def parse_datetime(val):
    if not val:
        return None
    for fmt in ("%d/%m/%Y %H:%M", "%d-%m-%Y %H:%M", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(val.strip(), fmt)
        except ValueError:
            continue
    return None


def parse_decimal(val):
    if not val:
        return None
    try:
        return Decimal(val.strip())
    except (InvalidOperation, ValueError):
        return None


# ── DB save (upsert by n_bl) ───────────────────────────────────────────────

async def save_to_db(db: AsyncSession, despacho: str, manifests: list[dict]) -> int:
    from app.database import Base
    from app.models.manifiesto_bl import ManifiestoBL

    count = 0
    for manifest in manifests:
        header = manifest["header"]
        for bl in manifest["bls"]:
            if not bl.get("n_bl"):
                continue

            existing = (
                await db.execute(
                    select(ManifiestoBL).where(ManifiestoBL.n_bl == bl["n_bl"])
                )
            ).scalar_one_or_none()

            new_almacen = bl.get("almacen")
            new_puerto = bl.get("puerto_desembarque")
            new_nave = header.get("nave")

            if existing:
                if (
                    existing.almacen == new_almacen
                    and existing.puerto_desembarque == new_puerto
                    and existing.nave == new_nave
                ):
                    continue
                existing.almacen = new_almacen
                existing.puerto_desembarque = new_puerto
                existing.nave = new_nave
                existing.despacho = despacho
                existing.nro_manifiesto = header.get("nro_manifiesto") or ""
                existing.sentido = header.get("sentido")
                existing.fecha_arribo_zarpe = parse_datetime(header.get("fecha_arribo_zarpe"))
                existing.cia_naviera = header.get("cia_naviera")
                existing.fecha_emision_manifiesto = parse_date(header.get("fecha_emision_manifiesto"))
                existing.fecha_aceptacion = parse_date(bl.get("fecha_aceptacion"))
                existing.total_peso = parse_decimal(bl.get("total_peso"))
                count += 1
            else:
                record = ManifiestoBL(
                    despacho=despacho,
                    nro_manifiesto=header.get("nro_manifiesto") or "",
                    nave=new_nave,
                    sentido=header.get("sentido"),
                    fecha_arribo_zarpe=parse_datetime(header.get("fecha_arribo_zarpe")),
                    cia_naviera=header.get("cia_naviera"),
                    fecha_emision_manifiesto=parse_date(header.get("fecha_emision_manifiesto")),
                    n_bl=bl["n_bl"],
                    almacen=new_almacen,
                    fecha_aceptacion=parse_date(bl.get("fecha_aceptacion")),
                    puerto_desembarque=new_puerto,
                    total_peso=parse_decimal(bl.get("total_peso")),
                )
                db.add(record)
                count += 1
    return count


# ── Interactive CLI ─────────────────────────────────────────────────────────

async def get_ports() -> list[str]:
    async with SisconSession() as db:
        result = await db.execute(text("""
            SELECT DISTINCT TRIM(pto_desembarque) as puerto, COUNT(*) as total
            FROM declaracion.archimp
            WHERE TRIM(pto_desembarque) <> ''
              AND fecha_arribo_estimado >= '2025-01-01'
              AND (__deleted IS NULL OR __deleted = 'false')
            GROUP BY TRIM(pto_desembarque)
            ORDER BY total DESC
        """))
        return [row[0] for row in result.fetchall()]


async def get_despachos(puerto: Optional[str], fecha_desde: date, fecha_hasta: date) -> list[tuple]:
    port_filter = "AND TRIM(pto_desembarque) = :puerto" if puerto else ""
    query = text(f"""
        SELECT despacho, numero_conocimiento, TRIM(pto_desembarque) as puerto,
               fecha_arribo_estimado, nombre_vehiculo
        FROM declaracion.archimp
        WHERE fecha_arribo_estimado BETWEEN :fecha_desde AND :fecha_hasta
          AND TRIM(numero_conocimiento) <> ''
          AND (__deleted IS NULL OR __deleted = 'false')
          {port_filter}
        ORDER BY fecha_arribo_estimado, despacho
    """)
    params = {"fecha_desde": fecha_desde, "fecha_hasta": fecha_hasta}
    if puerto:
        params["puerto"] = puerto

    async with SisconSession() as db:
        result = await db.execute(query, params)
        return result.fetchall()


def print_header(title: str):
    width = 90
    print(f"\n{CYAN}{'=' * width}")
    print(f"  {BOLD}{title}{RESET}{CYAN}")
    print(f"{'=' * width}{RESET}\n")


def print_separator():
    print(f"{DIM}{'─' * 90}{RESET}")


async def main():
    print_header("CONSULTA ALMACEN - ADUANA DE CHILE")

    # 1. Ask for dates
    print(f"{BOLD}Ingrese rango de fechas:{RESET}")
    while True:
        fecha_desde_str = input(f"  Fecha desde (YYYY-MM-DD) [{CYAN}2026-03-13{RESET}]: ").strip()
        if not fecha_desde_str:
            fecha_desde_str = "2026-03-13"
        try:
            fecha_desde = datetime.strptime(fecha_desde_str, "%Y-%m-%d").date()
            break
        except ValueError:
            print(f"  {RED}Formato invalido. Use YYYY-MM-DD{RESET}")

    while True:
        fecha_hasta_str = input(f"  Fecha hasta (YYYY-MM-DD) [{CYAN}2026-03-20{RESET}]: ").strip()
        if not fecha_hasta_str:
            fecha_hasta_str = "2026-03-20"
        try:
            fecha_hasta = datetime.strptime(fecha_hasta_str, "%Y-%m-%d").date()
            break
        except ValueError:
            print(f"  {RED}Formato invalido. Use YYYY-MM-DD{RESET}")

    # 2. Ask for port
    print(f"\n{BOLD}Puertos disponibles:{RESET}")
    ports = await get_ports()
    print(f"  {YELLOW}0{RESET} - TODOS LOS PUERTOS")
    for i, port in enumerate(ports, 1):
        print(f"  {YELLOW}{i}{RESET} - {port}")

    while True:
        choice = input(f"\n  Seleccione puerto [{CYAN}0{RESET}]: ").strip()
        if not choice:
            choice = "0"
        try:
            idx = int(choice)
            if 0 <= idx <= len(ports):
                break
        except ValueError:
            pass
        print(f"  {RED}Opcion invalida{RESET}")

    selected_port = ports[idx - 1] if idx > 0 else None
    port_label = selected_port or "TODOS"

    # 3. Fetch despachos
    print_header(f"Buscando despachos en {port_label} ({fecha_desde_str} a {fecha_hasta_str})")

    despachos = await get_despachos(selected_port, fecha_desde, fecha_hasta)

    if not despachos:
        print(f"  {RED}No se encontraron despachos para los criterios seleccionados.{RESET}")
        return

    print(f"  {GREEN}Se encontraron {BOLD}{len(despachos)}{RESET}{GREEN} despachos{RESET}\n")

    # 4. Show table header
    print(f"  {BOLD}{'Despacho':<10} {'Puerto':<18} {'ETA':<12} {'Nave/Vehiculo':<25} {'BL'}{RESET}")
    print_separator()
    for row in despachos:
        despacho, bl, puerto, eta, nave = row[0], (row[1] or "").strip(), row[2], row[3], (row[4] or "").strip()
        eta_str = eta.strftime("%Y-%m-%d") if eta else "-"
        nave_short = nave[:24] if nave else "-"
        bl_short = bl[:40] if bl else "-"
        print(f"  {despacho:<10} {puerto:<18} {eta_str:<12} {nave_short:<25} {bl_short}")
    print_separator()

    # 5. Ask to proceed
    proceed = input(f"\n  {BOLD}Consultar Aduana y guardar datos? (s/n) [{CYAN}s{RESET}]: ").strip().lower()
    if proceed and proceed != "s":
        print(f"\n  {YELLOW}Cancelado.{RESET}")
        return

    # 6. Process each despacho
    print_header("Consultando Aduana...")

    total_found = 0
    total_not_found = 0
    total_saved = 0

    async with AzureSession() as db:
        for i, row in enumerate(despachos, 1):
            despacho = row[0]
            bl_number = (row[1] or "").strip()
            puerto = row[2]

            progress = f"[{i}/{len(despachos)}]"
            sys.stdout.write(f"\r  {DIM}{progress}{RESET} Consultando {despacho} ... ")
            sys.stdout.flush()

            if not bl_number:
                print(f"{RED}Sin BL{RESET}")
                total_not_found += 1
                continue

            queries = split_bl(bl_number)
            despacho_found = False
            despacho_saved = 0

            for q in queries:
                manifests, error = await query_aduana(q)
                if manifests:
                    despacho_found = True
                    saved = await save_to_db(db, despacho, manifests)
                    despacho_saved += saved

                    for m in manifests:
                        for bl_detail in m["bls"]:
                            almacen = bl_detail.get("almacen") or "-"
                            nave = m["header"].get("nave") or "-"
                            pto = bl_detail.get("puerto_desembarque") or "-"
                            status = f"{GREEN}FOUND{RESET}"
                            saved_label = f" {YELLOW}(saved){RESET}" if saved > 0 else f" {DIM}(no changes){RESET}"
                            sys.stdout.write(f"\r  {DIM}{progress}{RESET} {despacho}  {status}  Nave: {BOLD}{nave}{RESET}  Almacen: {BOLD}{almacen}{RESET}  Puerto: {pto}{saved_label}\n")

            if not despacho_found:
                sys.stdout.write(f"\r  {DIM}{progress}{RESET} {despacho}  {RED}NOT FOUND{RESET}  BL: {bl_number[:50]}\n")
                total_not_found += 1
            else:
                total_found += 1
                total_saved += despacho_saved

        await db.commit()

    # 7. Summary
    print_header("RESUMEN")
    print(f"  Total despachos:    {BOLD}{len(despachos)}{RESET}")
    print(f"  Encontrados:        {GREEN}{BOLD}{total_found}{RESET}")
    print(f"  No encontrados:     {RED}{BOLD}{total_not_found}{RESET}")
    print(f"  Registros guardados:{YELLOW}{BOLD} {total_saved}{RESET}")
    print()


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore")
    asyncio.run(main())
