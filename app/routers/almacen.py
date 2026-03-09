import logging
import re
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from fastapi import APIRouter
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/almacen", tags=["almacen"])

ADUANA_URL = "https://isidora.aduana.cl/WebManifiestoMaritimo/Consultas/CON_BlsxMFTO.jsp?Action=Event"


# --- Response models ---

class AlmacenDetail(BaseModel):
    n_bl: Optional[str] = None
    almacen: Optional[str] = None
    fecha_aceptacion: Optional[str] = None
    puerto_desembarque: Optional[str] = None
    total_peso: Optional[str] = None


class ManifestHeader(BaseModel):
    nro_manifiesto: Optional[str] = None
    nave: Optional[str] = None
    sentido: Optional[str] = None
    fecha_arribo_zarpe: Optional[str] = None
    cia_naviera: Optional[str] = None
    fecha_emision_manifiesto: Optional[str] = None


class AlmacenManifest(BaseModel):
    header: ManifestHeader
    bls: list[AlmacenDetail] = []


class AlmacenPartResult(BaseModel):
    bl_query: str
    manifests: list[AlmacenManifest] = []
    error: Optional[str] = None


class AlmacenLookupResponse(BaseModel):
    bl_original: str
    parts: list[AlmacenPartResult]


# --- Parsing helpers ---

def _cell_text(cell) -> str:
    """Extract cleaned text from a table cell."""
    return cell.get_text(strip=True) if cell else ""


def _find_label_value(soup: BeautifulSoup, label: str) -> Optional[str]:
    """Find a cell containing `label` and return the next sibling cell's text."""
    for td in soup.find_all("td"):
        if label in td.get_text():
            nxt = td.find_next_sibling("td")
            if nxt:
                val = _cell_text(nxt)
                return val if val else None
    return None


def _is_bl_table(table) -> bool:
    """Check if a table is a BL detail table by looking for header cells."""
    for td in table.find_all("td", class_="SimpleObjectTableCellTitle", recursive=True):
        if "BL" in td.get_text():
            return True
    return False


def _parse_manifests(html: str) -> list[AlmacenManifest]:
    """Parse the Aduana HTML response and extract almacen info from BL details."""
    soup = BeautifulSoup(html, "html.parser")
    manifests: list[AlmacenManifest] = []

    tables = soup.find_all("table")

    current_header: Optional[ManifestHeader] = None
    current_bls: list[AlmacenDetail] = []

    for table in tables:
        table_class = " ".join(table.get("class", []))
        text = table.get_text()

        # Manifest header table
        if "SimpleObjectTableCell" in table_class and "DATOS CONSIGNADOS" in text:
            if current_header is not None:
                manifests.append(AlmacenManifest(header=current_header, bls=current_bls))
                current_bls = []

            current_header = ManifestHeader(
                nro_manifiesto=_find_label_value(table, "Nro. Manifiesto"),
                nave=_find_label_value(table, "Nave"),
                sentido=_find_label_value(table, "Sentido"),
                fecha_arribo_zarpe=_find_label_value(table, "Fecha Arribo/Zarpe"),
                cia_naviera=_find_label_value(table, "Naviera"),
                fecha_emision_manifiesto=_find_label_value(table, "Fecha Emisi"),
            )

        # BL detail table - extract almacen-focused fields
        elif "SimpleObjectTable" in table_class and _is_bl_table(table):
            rows = table.find_all("tr")
            for row in rows[1:]:  # skip header row
                cells = row.find_all("td")
                if len(cells) >= 10:
                    bl = AlmacenDetail(
                        n_bl=_cell_text(cells[0]) or None,
                        almacen=_cell_text(cells[5]) or None,
                        fecha_aceptacion=_cell_text(cells[3]) or None,
                        puerto_desembarque=_cell_text(cells[7]) or None,
                        total_peso=_cell_text(cells[9]) or None,
                    )
                    current_bls.append(bl)

    # Don't forget the last manifest
    if current_header is not None:
        manifests.append(AlmacenManifest(header=current_header, bls=current_bls))

    return manifests


async def _query_bl(bl: str) -> AlmacenPartResult:
    """Query a single BL number against the Aduana website."""
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
        logger.warning("Aduana timeout for BL %s", bl)
        return AlmacenPartResult(bl_query=bl, error="Aduana service timeout")
    except Exception as e:
        logger.warning("Aduana error for BL %s: %s: %s", bl, type(e).__name__, e)
        return AlmacenPartResult(bl_query=bl, error=f"Aduana service error: {type(e).__name__}: {e}")

    html = resp.content.decode("iso-8859-1")

    try:
        manifests = _parse_manifests(html)
    except Exception:
        logger.exception("Parse error for BL %s", bl)
        return AlmacenPartResult(bl_query=bl, error="Unable to parse response")

    if not manifests:
        return AlmacenPartResult(bl_query=bl, error="BL not found")

    return AlmacenPartResult(bl_query=bl, manifests=manifests)


# --- Endpoint ---

@router.get("/bl/{bl_number}", response_model=AlmacenLookupResponse)
async def lookup_almacen(
    bl_number: str,
):
    """Look up Almacen (warehouse) information from Chilean Customs (Aduana) by BL number."""
    bl_number = bl_number.strip()

    # Split on (H) or (N) notation
    upper = bl_number.upper()
    markers = re.findall(r'\([HN]\)', upper)
    if markers:
        parts_raw = re.split(r'(\([HN]\))', upper)
        queries = []
        for i, part in enumerate(parts_raw):
            part = part.strip()
            if not part:
                continue
            if re.match(r'\([HN]\)$', part):
                continue
            if i > 0 and re.match(r'\([HN]\)$', parts_raw[i - 1]):
                queries.append(parts_raw[i - 1] + part)
            else:
                queries.append(part)
    else:
        queries = [bl_number]

    parts: list[AlmacenPartResult] = []
    for q in queries:
        result = await _query_bl(q)
        parts.append(result)

    return AlmacenLookupResponse(bl_original=bl_number, parts=parts)
