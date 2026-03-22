import logging
from contextlib import asynccontextmanager
from datetime import date, timedelta

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from app.database import engine, Base
from app.models.almacen_maestro import AlmacenMaestro  # noqa: F401
from app.models.manifiesto_bl import ManifiestoBL  # noqa: F401
from app.routers import almacen

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    except Exception as e:
        logging.warning("Could not run create_all on startup: %s", e)
    yield


app = FastAPI(
    title="API Almacen Despacho",
    description="Consulta de almacen desde Aduana y persistencia en manifiesto_bl",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(almacen.router)


@app.get("/")
async def root():
    today_str = date.today().isoformat()
    plus7_str = (date.today() + timedelta(days=7)).isoformat()
    content = """<!DOCTYPE html>
<html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Almacen Despacho</title>
<script src="https://cdn.jsdelivr.net/npm/keycloak-js@25/dist/keycloak.min.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#f0f2f5;color:#1a1a2e}
.header{background:linear-gradient(135deg,#1a1a2e,#16213e);color:#fff;padding:18px 30px;display:flex;align-items:center;justify-content:space-between}
.header h1{font-size:1.3rem;font-weight:600}
.header .badge{background:#e94560;padding:4px 12px;border-radius:12px;font-size:.75rem}
.tabs{display:flex;background:#fff;border-bottom:2px solid #e5e7eb;padding:0 20px}
.tab{padding:12px 24px;cursor:pointer;font-size:.85rem;font-weight:600;color:#888;border-bottom:3px solid transparent;margin-bottom:-2px;transition:.2s}
.tab:hover{color:#333}
.tab.active{color:#3b82f6;border-bottom-color:#3b82f6}
.container{max-width:1600px;margin:0 auto;padding:16px 20px}
.panel{display:none}.panel.active{display:block}
.filters{background:#fff;border-radius:10px;padding:16px 20px;margin-bottom:16px;display:flex;gap:12px;align-items:end;flex-wrap:wrap;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.filters label{font-size:.78rem;font-weight:600;color:#555;display:block;margin-bottom:4px}
.filters input,.filters select{padding:8px 12px;border:1px solid #ddd;border-radius:6px;font-size:.85rem;outline:none;min-width:140px}
.filters input:focus,.filters select:focus{border-color:#3b82f6}
.btn{padding:8px 20px;border:none;border-radius:6px;font-size:.85rem;font-weight:600;cursor:pointer;transition:.2s}
.btn-primary{background:#3b82f6;color:#fff}.btn-primary:hover{background:#2563eb}
.btn-secondary{background:#6b7280;color:#fff}.btn-secondary:hover{background:#4b5563}
.btn-success{background:#10b981;color:#fff}.btn-success:hover{background:#059669}
.btn:disabled{opacity:.5;cursor:not-allowed}
.stats{display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap}
.stat{background:#fff;border-radius:10px;padding:14px 20px;flex:1;min-width:130px;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.stat .num{font-size:1.5rem;font-weight:700}.stat .lbl{font-size:.73rem;color:#888;margin-top:2px}
.stat.green .num{color:#10b981}.stat.red .num{color:#ef4444}.stat.blue .num{color:#3b82f6}.stat.yellow .num{color:#f59e0b}
.table-wrap{background:#fff;border-radius:10px;overflow-x:auto;box-shadow:0 1px 3px rgba(0,0,0,.08)}
table{width:100%;border-collapse:collapse;font-size:.8rem}
th{background:#f8f9fa;padding:10px 12px;text-align:left;font-weight:600;color:#555;border-bottom:2px solid #e5e7eb;white-space:nowrap;position:sticky;top:0}
td{padding:8px 12px;border-bottom:1px solid #f0f0f0;max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
tr:hover td{background:#f8faff}
.tag{display:inline-block;padding:2px 8px;border-radius:4px;font-size:.72rem;font-weight:600}
.tag-found{background:#dcfce7;color:#166534}.tag-not-found{background:#fee2e2;color:#991b1b}
.tag-saved{background:#fef3c7;color:#92400e}.tag-blue{background:#dbeafe;color:#1e40af}
.tag-green{background:#dcfce7;color:#166534}.tag-gray{background:#f3f4f6;color:#374151}
.tag-edited{background:#fef3c7;color:#92400e}
.empty{text-align:center;padding:50px 20px;color:#999}
.loading{text-align:center;padding:40px;color:#888}
.loading::after{content:'';display:inline-block;width:18px;height:18px;border:2px solid #ddd;border-top-color:#3b82f6;border-radius:50%;animation:spin .6s linear infinite;margin-left:8px;vertical-align:middle}
@keyframes spin{to{transform:rotate(360deg)}}
.progress{background:#fff;border-radius:10px;padding:20px;margin-bottom:16px;box-shadow:0 1px 3px rgba(0,0,0,.08);display:none}
.progress.show{display:block}
.progress-bar{background:#e5e7eb;border-radius:6px;height:8px;overflow:hidden;margin-top:8px}
.progress-fill{background:linear-gradient(90deg,#3b82f6,#10b981);height:100%;border-radius:6px;transition:width .3s}
.progress-text{font-size:.82rem;color:#555;margin-bottom:4px}
.sub-row{background:#f8faff}
.sub-row td{padding:6px 12px 6px 40px;font-size:.78rem;border-bottom:1px solid #eef2ff}
.login-bar{background:#fff;border-radius:10px;padding:12px 20px;margin-bottom:16px;display:flex;align-items:center;justify-content:space-between;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.login-bar .user-info{font-size:.85rem;color:#555}
.login-bar .user-info strong{color:#1a1a2e}
.btn-login{background:#e94560;color:#fff;padding:8px 20px;border:none;border-radius:6px;font-size:.85rem;font-weight:600;cursor:pointer}
.btn-login:hover{background:#c73650}
.btn-logout{background:#6b7280;color:#fff;padding:8px 16px;border:none;border-radius:6px;font-size:.82rem;cursor:pointer}
.btn-logout:hover{background:#4b5563}
.edit-input{padding:4px 8px;border:1px solid #ddd;border-radius:4px;font-size:.8rem;width:120px}
.edit-input:focus{border-color:#3b82f6;outline:none}
.btn-save-cell{padding:3px 10px;background:#10b981;color:#fff;border:none;border-radius:4px;font-size:.72rem;font-weight:600;cursor:pointer;margin-left:4px}
.btn-save-cell:hover{background:#059669}
.btn-save-cell:disabled{opacity:.5;cursor:not-allowed}
.save-ok{color:#10b981;font-size:.75rem;margin-left:4px}
</style></head><body>
<div id="appContent">
<div class="header">
  <h1>Almacen Despacho - Aduana de Chile</h1>
  <div style="display:flex;align-items:center;gap:12px">
    <span id="headerUser" style="font-size:.82rem;opacity:.8"></span>
    <button class="btn-login" id="btnLogin" onclick="doLogin()" style="display:none">Iniciar Sesion</button>
    <button class="btn-logout" id="btnLogout" onclick="doLogout()" style="display:none">Cerrar Sesion</button>
    <span class="badge">ADUANA</span>
  </div>
</div>
<div class="tabs">
  <div class="tab active" onclick="switchTab('consulta')">Consulta Aduana</div>
  <div class="tab" onclick="switchTab('registros')">Almacenes/Sidemar</div>
  <div class="tab" onclick="switchTab('gestion')">Actualiza Inscripcion</div>
  <div class="tab" onclick="switchTab('maestro')">Almacenes</div>
</div>
<div class="container">

<!-- TAB 1: Consulta -->
<div id="panel-consulta" class="panel active">
  <div class="filters">
    <div><label>Fecha Desde</label><input id="fDesde" type="date" value="2026-03-13"></div>
    <div><label>Fecha Hasta</label><input id="fHasta" type="date" value="2026-03-20"></div>
    <div><label>Puerto</label><select id="fPuerto"><option value="">Todos los puertos</option></select></div>
    <div><button class="btn btn-primary" id="btnConsultar" onclick="runBatch()">Consultar Aduana</button></div>
  </div>
  <div class="progress" id="progress">
    <div class="progress-text" id="progressText">Consultando...</div>
    <div class="progress-bar"><div class="progress-fill" id="progressFill" style="width:0%"></div></div>
  </div>
  <div class="stats" id="batchStats"></div>
  <div class="table-wrap"><table>
    <thead><tr>
      <th></th><th>Estado</th><th>Despacho</th><th>Puerto</th><th>ETA</th>
      <th>Nave/Vehiculo</th><th>BL (Conocimiento)</th><th>Almacen</th>
      <th>Nave (Aduana)</th><th>Puerto Destino</th><th>Cia Naviera</th><th>Peso</th>
    </tr></thead>
    <tbody id="batchBody"><tr><td colspan="12" class="empty">Seleccione fechas y puerto, luego presione "Consultar Aduana"</td></tr></tbody>
  </table></div>
</div>

<!-- TAB 2: Registros -->
<div id="panel-registros" class="panel">
  <div class="filters">
    <div><label>Fecha Desde</label><input id="rDesde" type="date" value="__TODAY__"></div>
    <div><label>Fecha Hasta</label><input id="rHasta" type="date" value="__PLUS7__"></div>
    <div><label>Puerto</label><select id="rPuerto"><option value="">Todos los puertos</option></select></div>
    <div><button class="btn btn-primary" onclick="loadRecords()">Buscar</button></div>
    <div><button class="btn btn-success" onclick="downloadExcel()">Descargar Excel</button></div>
    <div style="border-left:1px solid #ddd;padding-left:12px"><label>Filtrar</label><input id="search" type="text" placeholder="BL / Despacho..." style="width:160px"></div>
    <div><label>Estado</label><select id="filterStatus"><option value="">Todos</option><option value="found">Encontrados</option><option value="not_found">No Encontrados</option></select></div>
  </div>
  <div class="stats" id="recStats"></div>
  <div class="table-wrap"><table>
    <thead><tr>
      <th></th><th>Estado</th><th>Despacho</th><th>Importador</th><th>Puerto</th><th>ETA</th><th>Nave/Vehiculo</th>
      <th>BL (Conocimiento)</th><th>Nro BL (Aduana)</th><th>Nave (Aduana)</th><th>Almacen</th>
      <th>Puerto Destino</th><th>Cia Naviera</th><th>Peso</th><th>Actualizado</th>
    </tr></thead>
    <tbody id="recBody"><tr><td colspan="15" class="empty">Seleccione fechas y presione "Buscar"</td></tr></tbody>
  </table></div>
</div>

<!-- TAB 3: Gestion Registros -->
<div id="panel-gestion" class="panel">
  <div class="filters">
    <div><label>Fecha Desde</label><input id="gDesde" type="date" value="__TODAY__"></div>
    <div><label>Fecha Hasta</label><input id="gHasta" type="date" value="__PLUS7__"></div>
    <div><label>Puerto</label><select id="gPuerto"><option value="">Todos los puertos</option></select></div>
    <div><button class="btn btn-primary" onclick="loadGestionRecords()">Buscar</button></div>
    <div><button class="btn btn-success" onclick="downloadExcelG()">Descargar Excel</button></div>
    <div style="border-left:1px solid #ddd;padding-left:12px"><label>Filtrar</label><input id="gSearch" type="text" placeholder="BL / Despacho / Importador..." style="width:180px"></div>
    <div><label>Estado</label><select id="gFilterStatus"><option value="">Todos</option><option value="found">Encontrados</option><option value="not_found">No Encontrados</option></select></div>
  </div>
  <div class="stats" id="gStats"></div>
  <div class="table-wrap"><table>
    <thead><tr>
      <th></th><th>Estado</th><th>Despacho</th><th>Importador</th><th>Puerto</th><th>ETA</th><th>Nave/Vehiculo</th>
      <th>BL (Conocimiento)</th><th>Nro BL (Aduana)</th><th>Nave (Aduana)</th><th>Almacen (Aduana)</th>
      <th>Almacen Real</th><th>Puerto Destino</th><th>Cia Naviera</th><th>Peso</th>
      <th>Modificado Por</th><th>Fecha Modificacion</th>
    </tr></thead>
    <tbody id="gestionBody"><tr><td colspan="17" class="empty">Seleccione fechas y presione "Buscar"</td></tr></tbody>
  </table></div>
</div>

<!-- TAB 4: Almacenes Maestro -->
<div id="panel-maestro" class="panel">
  <div class="filters">
    <div><label>Nombre</label><input id="mNuevoNombre" type="text" placeholder="Nombre del almacen..." style="width:220px"></div>
    <div><label>Puerto / Aeropuerto</label><input id="mNuevoPuerto" type="text" placeholder="Puerto o aeropuerto..." style="width:200px"></div>
    <div><button class="btn btn-primary" onclick="addAlmacen()">Agregar</button></div>
    <div style="border-left:1px solid #ddd;padding-left:12px">
      <button class="btn btn-secondary" onclick="seedAlmacenes()">Importar desde Registros</button>
    </div>
  </div>
  <div id="maestroMsg" style="margin-bottom:12px"></div>
  <div class="table-wrap"><table>
    <thead><tr><th>#</th><th>Nombre</th><th>Puerto / Aeropuerto</th><th>Acciones</th></tr></thead>
    <tbody id="maestroBody"><tr><td colspan="4" class="empty">Cargando...</td></tr></tbody>
  </table></div>
</div>

</div>
</div>
<script>
// ── Tab switching ──
const tabLabels={'consulta':'Consulta Aduana','registros':'Almacenes/Sidemar','gestion':'Actualiza Inscripcion','maestro':'Almacenes'};
function switchTab(name){
  document.querySelectorAll('.tab').forEach(t=>t.classList.toggle('active',t.textContent.trim()===tabLabels[name]));
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));
  document.getElementById('panel-'+name).classList.add('active');
  if(name==='registros'&&!allRecords.length)loadRecords();
  if(name==='gestion'&&!gestionRecords.length)loadGestionRecords();
  if(name==='maestro'&&!maestroLoaded)loadMaestro();
}

// ── Authenticated fetch helper ──
async function authFetch(url,opts={}){
  const token=await getValidToken();
  if(!token)throw new Error('No autenticado');
  if(!opts.headers)opts.headers={};
  opts.headers['Authorization']='Bearer '+token;
  return fetch(url,opts);
}

// ── Load ports (shared) ──
async function populatePorts(selId){
  try{
    const r=await authFetch('/almacen/ports');
    const ports=await r.json();
    const sel=document.getElementById(selId);
    ports.forEach(p=>{const o=document.createElement('option');o.value=p.puerto;o.textContent=`${p.puerto} (${p.total})`;sel.appendChild(o)});
  }catch(e){}
}

// ── Load almacenes list ──
let almacenesList=[];
async function loadAlmacenesList(){
  try{
    const r=await authFetch('/almacen/almacenes-list?_t='+Date.now());
    if(!r.ok){console.error('almacenes API error:',r.status,await r.text());almacenesList=[];return;}
    const data=await r.json();
    if(!Array.isArray(data)){console.error('almacenes API returned non-array:',data);almacenesList=[];return;}
    almacenesList=data;
    console.log('almacenesList loaded from almacen_maestro:',almacenesList.length,'items');
  }catch(e){console.error('Failed to load almacenes',e);almacenesList=[];}
}

// ── Keycloak (login required on page load) ──
let keycloak=null;
let authToken=null;
let currentUsername=null;

async function initApp(){
  keycloak=new Keycloak({
    url:'https://hercules.ajleon.cl/auth',
    realm:'clientes-prod',
    clientId:'apicex-web'
  });
  try{
    const ok=await keycloak.init({onLoad:'login-required',checkLoginIframe:false});
    authToken=keycloak.token;
    currentUsername=keycloak.tokenParsed.email||keycloak.tokenParsed.preferred_username||'user';
    document.getElementById('headerUser').textContent=currentUsername;
    document.getElementById('btnLogout').style.display='inline-block';
    document.getElementById('btnLogin').style.display='none';
    populatePorts('fPuerto');populatePorts('rPuerto');populatePorts('gPuerto');
    await loadAlmacenesList();
    await checkAduanaEnabled();
  }catch(e){
    console.error('Keycloak init failed',e);
    document.getElementById('btnLogin').style.display='inline-block';
  }
}

async function checkAduanaEnabled(){
  try{
    const r=await authFetch('/almacen/config');
    const cfg=await r.json();
    if(!cfg.aduana_enabled){
      document.querySelectorAll('.tab').forEach(t=>{if(t.textContent.trim()==='Consulta Aduana'){t.style.display='none';}});
      document.getElementById('panel-consulta').classList.remove('active');
      document.querySelectorAll('.tab').forEach(t=>{if(t.textContent.trim()==='Almacenes/Sidemar'){t.classList.add('active');}});
      document.getElementById('panel-registros').classList.add('active');
      loadRecords();
    }
  }catch(e){console.error('Config check failed',e);}
}

function doLogin(){
  if(keycloak)keycloak.login();
}

function doLogout(){
  if(keycloak)keycloak.logout({redirectUri:window.location.origin});
}

async function getValidToken(){
  if(!keycloak||!keycloak.authenticated){
    if(keycloak)keycloak.login();
    return null;
  }
  try{
    await keycloak.updateToken(30);
    authToken=keycloak.token;
    return authToken;
  }catch(e){
    keycloak.login();
    return null;
  }
}

initApp();

// ── Batch consulta (Tab 1) ──
async function runBatch(){
  const desde=document.getElementById('fDesde').value;
  const hasta=document.getElementById('fHasta').value;
  const puerto=document.getElementById('fPuerto').value;
  const btn=document.getElementById('btnConsultar');
  const prog=document.getElementById('progress');
  const tbody=document.getElementById('batchBody');

  btn.disabled=true;btn.textContent='Consultando...';
  prog.classList.add('show');
  document.getElementById('progressText').textContent='Conectando con Aduana...';
  document.getElementById('progressFill').style.width='10%';
  tbody.innerHTML='<tr><td colspan="12" class="loading">Consultando Aduana</td></tr>';
  document.getElementById('batchStats').innerHTML='';

  try{
    const params=new URLSearchParams({fecha_desde:desde,fecha_hasta:hasta});
    if(puerto)params.set('puerto',puerto);
    document.getElementById('progressFill').style.width='30%';
    document.getElementById('progressText').textContent='Buscando despachos y consultando Aduana...';

    const r=await authFetch('/almacen/batch-update?'+params);
    const data=await r.json();

    document.getElementById('progressFill').style.width='100%';
    document.getElementById('progressText').textContent='Completado';

    document.getElementById('batchStats').innerHTML=`
      <div class="stat blue"><div class="num">${data.total_despachos}</div><div class="lbl">Total Despachos</div></div>
      <div class="stat green"><div class="num">${data.total_found}</div><div class="lbl">Encontrados</div></div>
      <div class="stat red"><div class="num">${data.total_not_found}</div><div class="lbl">No Encontrados</div></div>
      <div class="stat yellow"><div class="num">${data.total_saved}</div><div class="lbl">Guardados / Actualizados</div></div>`;

    if(!data.results.length){
      tbody.innerHTML='<tr><td colspan="12" class="empty">No se encontraron despachos para el rango seleccionado</td></tr>';
    } else {
      let html='';
      data.results.forEach(r=>{
        const isFound=r.status==='found';
        const statusTag=isFound?'<span class="tag tag-found">ENCONTRADO</span>':'<span class="tag tag-not-found">NO ENCONTRADO</span>';
        const icon=isFound?'&#9989;':'&#10060;';
        if(isFound && r.bls.length){
          const first=r.bls[0];
          const savedTag=first.saved?'<span class="tag tag-saved">Guardado</span>':'';
          html+=`<tr>
            <td>${icon}</td><td>${statusTag}</td>
            <td><strong>${r.despacho}</strong></td><td>${r.puerto||'-'}</td><td>${r.eta||'-'}</td>
            <td>${r.nombre_vehiculo||'-'}</td>
            <td><span class="tag tag-blue">${first.n_bl||'-'}</span></td>
            <td><span class="tag tag-green">${first.almacen||'-'}</span></td>
            <td>${first.nave||'-'}</td><td>${first.puerto_desembarque||'-'}</td>
            <td>${first.cia_naviera||'-'}</td><td>${first.total_peso||'-'} ${savedTag}</td></tr>`;
          for(let i=1;i<r.bls.length;i++){
            const bl=r.bls[i];
            const sTag=bl.saved?'<span class="tag tag-saved">Guardado</span>':'';
            html+=`<tr class="sub-row">
              <td></td><td></td><td></td><td></td><td></td><td></td>
              <td><span class="tag tag-blue">${bl.n_bl||'-'}</span></td>
              <td><span class="tag tag-green">${bl.almacen||'-'}</span></td>
              <td>${bl.nave||'-'}</td><td>${bl.puerto_desembarque||'-'}</td>
              <td>${bl.cia_naviera||'-'}</td><td>${bl.total_peso||'-'} ${sTag}</td></tr>`;
          }
        } else {
          html+=`<tr style="opacity:${isFound?1:.6}">
            <td>${icon}</td><td>${statusTag}</td>
            <td><strong>${r.despacho}</strong></td><td>${r.puerto||'-'}</td><td>${r.eta||'-'}</td>
            <td>${r.nombre_vehiculo||'-'}</td>
            <td><span class="tag tag-gray">${r.numero_conocimiento||'-'}</span></td>
            <td colspan="5" style="color:#999">${r.error||'-'}</td></tr>`;
        }
      });
      tbody.innerHTML=html;
    }
    setTimeout(()=>prog.classList.remove('show'),2000);
  }catch(e){
    tbody.innerHTML='<tr><td colspan="12" class="empty">Error: '+e.message+'</td></tr>';
    prog.classList.remove('show');
  }
  btn.disabled=false;btn.textContent='Consultar Aduana';
}

// ── Records tab (Tab 2) ──
let allRecords=[];
async function loadRecords(){
  const desde=document.getElementById('rDesde').value;
  const hasta=document.getElementById('rHasta').value;
  const puerto=document.getElementById('rPuerto').value;
  const tbody=document.getElementById('recBody');
  tbody.innerHTML='<tr><td colspan="15" class="loading">Cargando</td></tr>';
  document.getElementById('recStats').innerHTML='';
  try{
    const params=new URLSearchParams({fecha_desde:desde,fecha_hasta:hasta});
    if(puerto)params.set('puerto',puerto);
    const r=await authFetch('/almacen/registros?'+params);
    const data=await r.json();
    allRecords=data.items;
    renderRecords();
  }catch(e){tbody.innerHTML='<tr><td colspan="15" class="empty">Error cargando datos</td></tr>'}
}
function renderRecords(){
  const q=document.getElementById('search').value.toLowerCase();
  const fS=document.getElementById('filterStatus').value;
  let data=allRecords.filter(r=>{
    if(q&&!((r.n_bl||'').toLowerCase().includes(q)||(r.despacho||'').toLowerCase().includes(q)||(r.numero_conocimiento||'').toLowerCase().includes(q)||(r.nombre_importador||'').toLowerCase().includes(q)))return false;
    if(fS&&r.status!==fS)return false;
    return true;
  });
  const found=data.filter(r=>r.status==='found').length;
  const notFound=data.filter(r=>r.status==='not_found').length;
  document.getElementById('recStats').innerHTML=`
    <div class="stat blue"><div class="num">${data.length}</div><div class="lbl">Total Registros</div></div>
    <div class="stat green"><div class="num">${found}</div><div class="lbl">Encontrados</div></div>
    <div class="stat red"><div class="num">${notFound}</div><div class="lbl">No Encontrados</div></div>
    <div class="stat"><div class="num">${new Set(data.map(r=>r.despacho).filter(Boolean)).size}</div><div class="lbl">Despachos</div></div>`;
  const tbody=document.getElementById('recBody');
  if(!data.length){tbody.innerHTML='<tr><td colspan="15" class="empty">No se encontraron registros</td></tr>';return}
  tbody.innerHTML=data.map(r=>{
    const isFound=r.status==='found';
    const icon=isFound?'&#9989;':'&#10060;';
    const statusTag=isFound?'<span class="tag tag-found">ENCONTRADO</span>':'<span class="tag tag-not-found">NO ENCONTRADO</span>';
    const rowStyle=isFound?'':'style="background:#fff5f5"';
    return `<tr ${rowStyle}>
      <td>${icon}</td><td>${statusTag}</td>
      <td><strong>${r.despacho||'-'}</strong></td>
      <td>${r.nombre_importador||'-'}</td>
      <td>${r.puerto||'-'}</td><td>${r.eta||'-'}</td>
      <td>${r.nombre_vehiculo||'-'}</td>
      <td><span class="tag ${isFound?'tag-blue':'tag-gray'}">${r.numero_conocimiento||'-'}</span></td>
      <td>${isFound?(r.n_bl||'-'):'-'}</td>
      <td>${r.nave||'-'}</td>
      <td>${isFound?'<span class="tag tag-green">'+(r.almacen||'-')+'</span>':'-'}</td>
      <td>${r.puerto_desembarque||'-'}</td>
      <td>${r.cia_naviera||'-'}</td>
      <td>${r.total_peso||'-'}</td>
      <td>${r.updated_at?new Date(r.updated_at).toLocaleString('es-CL'):'-'}</td></tr>`;
  }).join('');
}
async function downloadExcel(){
  const desde=document.getElementById('rDesde').value;
  const hasta=document.getElementById('rHasta').value;
  const puerto=document.getElementById('rPuerto').value;
  const params=new URLSearchParams({fecha_desde:desde,fecha_hasta:hasta});
  if(puerto)params.set('puerto',puerto);
  try{
    const r=await authFetch('/almacen/registros/excel?'+params);
    const blob=await r.blob();
    const url=URL.createObjectURL(blob);
    const a=document.createElement('a');a.href=url;
    const cd=r.headers.get('content-disposition')||'';
    const fn=cd.match(/filename=(.+)/)?.[1]||'registros.xlsx';
    a.download=fn;a.click();URL.revokeObjectURL(url);
  }catch(e){alert('Error descargando Excel: '+e.message);}
}
document.getElementById('search').addEventListener('input',renderRecords);
document.getElementById('filterStatus').addEventListener('change',renderRecords);

// ── Gestion Registros tab (Tab 3) ──
let gestionRecords=[];
async function loadGestionRecords(){
  const desde=document.getElementById('gDesde').value;
  const hasta=document.getElementById('gHasta').value;
  const puerto=document.getElementById('gPuerto').value;
  const tbody=document.getElementById('gestionBody');
  tbody.innerHTML='<tr><td colspan="17" class="loading">Cargando</td></tr>';
  document.getElementById('gStats').innerHTML='';
  try{
    await loadAlmacenesList();
    const params=new URLSearchParams({fecha_desde:desde,fecha_hasta:hasta});
    if(puerto)params.set('puerto',puerto);
    const r=await authFetch('/almacen/registros?'+params);
    const data=await r.json();
    gestionRecords=data.items;
    renderGestion();
  }catch(e){tbody.innerHTML='<tr><td colspan="17" class="empty">Error cargando datos</td></tr>'}
}

function renderGestion(){
  const q=document.getElementById('gSearch').value.toLowerCase();
  const fS=document.getElementById('gFilterStatus').value;
  let data=gestionRecords.filter(r=>{
    if(q&&!((r.n_bl||'').toLowerCase().includes(q)||(r.despacho||'').toLowerCase().includes(q)||(r.numero_conocimiento||'').toLowerCase().includes(q)||(r.nombre_importador||'').toLowerCase().includes(q)||(r.almacen_real||'').toLowerCase().includes(q)))return false;
    if(fS&&r.status!==fS)return false;
    return true;
  });
  const found=data.filter(r=>r.status==='found').length;
  const notFound=data.filter(r=>r.status==='not_found').length;
  const edited=data.filter(r=>r.almacen_real).length;
  document.getElementById('gStats').innerHTML=`
    <div class="stat blue"><div class="num">${data.length}</div><div class="lbl">Total Registros</div></div>
    <div class="stat green"><div class="num">${found}</div><div class="lbl">Encontrados</div></div>
    <div class="stat red"><div class="num">${notFound}</div><div class="lbl">No Encontrados</div></div>
    <div class="stat yellow"><div class="num">${edited}</div><div class="lbl">Con Almacen Real</div></div>`;
  const tbody=document.getElementById('gestionBody');
  const isLoggedIn=!!authToken;
  if(!data.length){tbody.innerHTML='<tr><td colspan="17" class="empty">No se encontraron registros</td></tr>';return}
  tbody.innerHTML=data.map(r=>{
    const isFound=r.status==='found';
    const icon=isFound?'&#9989;':'&#10060;';
    const statusTag=isFound?'<span class="tag tag-found">ENCONTRADO</span>':'<span class="tag tag-not-found">NO ENCONTRADO</span>';
    const rowStyle=isFound?'':'style="background:#fff5f5"';

    let almacenRealCell='-';
    if(isFound&&r.id){
      const val=r.almacen_real||r.almacen||'';
      const hasEdit=r.almacen_real?'<span class="tag tag-edited">editado</span> ':'';
      let opts='<option value="">-- Seleccionar --</option>';
      almacenesList.forEach(a=>{
        const sel=a.nombre===val?' selected':'';
        const label=a.puerto?a.nombre+' ('+a.puerto+')':a.nombre;
        opts+=`<option value="${a.nombre.replace(/"/g,'&quot;')}"${sel}>${label}</option>`;
      });
      almacenRealCell=`<div style="display:flex;align-items:center;gap:4px;white-space:nowrap">${hasEdit}<select class="edit-input" id="ar-${r.id}" style="width:200px">${opts}</select>
        <button class="btn-save-cell" id="btn-${r.id}" onclick="saveAlmacenReal(${r.id})">Guardar</button>
        <span id="ok-${r.id}"></span></div>`;
    }

    const modBy=r.usuario_actualizacion||'-';
    const modDate=r.fecha_actualizacion_manual?new Date(r.fecha_actualizacion_manual).toLocaleString('es-CL'):'-';

    return `<tr ${rowStyle}>
      <td>${icon}</td><td>${statusTag}</td>
      <td><strong>${r.despacho||'-'}</strong></td>
      <td>${r.nombre_importador||'-'}</td>
      <td>${r.puerto||'-'}</td><td>${r.eta||'-'}</td>
      <td>${r.nombre_vehiculo||'-'}</td>
      <td><span class="tag ${isFound?'tag-blue':'tag-gray'}">${r.numero_conocimiento||'-'}</span></td>
      <td>${isFound?(r.n_bl||'-'):'-'}</td>
      <td>${r.nave||'-'}</td>
      <td>${isFound?'<span class="tag tag-green">'+(r.almacen||'-')+'</span>':'-'}</td>
      <td style="overflow:visible;max-width:none">${almacenRealCell}</td>
      <td>${r.puerto_desembarque||'-'}</td>
      <td>${r.cia_naviera||'-'}</td>
      <td>${r.total_peso||'-'}</td>
      <td>${modBy}</td>
      <td>${modDate}</td></tr>`;
  }).join('');
}

async function saveAlmacenReal(recordId){
  const input=document.getElementById('ar-'+recordId);
  const btn=document.getElementById('btn-'+recordId);
  const okSpan=document.getElementById('ok-'+recordId);
  const newVal=input.value.trim();
  if(!newVal){alert('El valor de Almacen Real no puede estar vacio');return;}

  const token=await getValidToken();
  if(!token){alert('Debe iniciar sesion para guardar cambios');return;}

  btn.disabled=true;btn.textContent='...';okSpan.textContent='';
  try{
    const r=await fetch(`/almacen/registros/${recordId}`,{
      method:'PUT',
      headers:{'Content-Type':'application/json','Authorization':'Bearer '+token},
      body:JSON.stringify({almacen_real:newVal})
    });
    if(!r.ok){const err=await r.json();alert('Error: '+(err.detail||'Error desconocido'));btn.disabled=false;btn.textContent='Guardar';return;}
    const data=await r.json();
    const rec=gestionRecords.find(g=>g.id===recordId);
    if(rec){
      rec.almacen_real=data.almacen_real;
      rec.usuario_actualizacion=data.usuario_actualizacion;
      rec.fecha_actualizacion_manual=data.fecha_actualizacion_manual;
    }
    btn.disabled=false;btn.textContent='Guardar';
    okSpan.innerHTML='<span class="save-ok">&#10003; Guardado</span>';
    setTimeout(()=>{if(okSpan)okSpan.textContent='';},3000);
  }catch(e){
    alert('Error de conexion: '+e.message);
    btn.disabled=false;btn.textContent='Guardar';
  }
}

async function downloadExcelG(){
  const desde=document.getElementById('gDesde').value;
  const hasta=document.getElementById('gHasta').value;
  const puerto=document.getElementById('gPuerto').value;
  const params=new URLSearchParams({fecha_desde:desde,fecha_hasta:hasta});
  if(puerto)params.set('puerto',puerto);
  try{
    const r=await authFetch('/almacen/registros/excel?'+params);
    const blob=await r.blob();
    const url=URL.createObjectURL(blob);
    const a=document.createElement('a');a.href=url;
    const cd=r.headers.get('content-disposition')||'';
    const fn=cd.match(/filename=(.+)/)?.[1]||'registros.xlsx';
    a.download=fn;a.click();URL.revokeObjectURL(url);
  }catch(e){alert('Error descargando Excel: '+e.message);}
}

document.getElementById('gSearch').addEventListener('input',renderGestion);
document.getElementById('gFilterStatus').addEventListener('change',renderGestion);

// ── Almacenes Maestro tab (Tab 4) ──
let maestroData=[];
let maestroLoaded=false;

async function loadMaestro(){
  const tbody=document.getElementById('maestroBody');
  tbody.innerHTML='<tr><td colspan="4" class="loading">Cargando</td></tr>';
  try{
    const r=await authFetch('/almacen/almacenes-list?_t='+Date.now());
    maestroData=await r.json();
    maestroLoaded=true;
    renderMaestro();
  }catch(e){tbody.innerHTML='<tr><td colspan="4" class="empty">Error cargando almacenes</td></tr>';}
}

function renderMaestro(){
  const tbody=document.getElementById('maestroBody');
  if(!maestroData.length){tbody.innerHTML='<tr><td colspan="4" class="empty">No hay almacenes registrados. Use "Importar desde Registros" para poblar la lista.</td></tr>';return;}
  tbody.innerHTML=maestroData.map((a,i)=>`<tr>
    <td>${i+1}</td>
    <td id="td-n-${a.id}">${a.nombre}</td>
    <td id="td-p-${a.id}">${a.puerto||'-'}</td>
    <td style="white-space:nowrap">
      <button class="btn btn-primary" style="padding:4px 12px;font-size:.78rem" onclick="startEditAlmacen(${a.id})">Editar</button>
      <button class="btn btn-secondary" style="padding:4px 12px;font-size:.78rem;margin-left:4px" onclick="deleteAlmacen(${a.id},'${a.nombre.replace(/'/g,"\\'")}')">Eliminar</button>
    </td></tr>`).join('');
}

function startEditAlmacen(id){
  const rec=maestroData.find(a=>a.id===id);
  if(!rec)return;
  const tdN=document.getElementById('td-n-'+id);
  const tdP=document.getElementById('td-p-'+id);
  tdN.innerHTML=`<input class="edit-input" id="edit-n-${id}" value="${rec.nombre.replace(/"/g,'&quot;')}" style="width:200px">`;
  tdP.innerHTML=`<div style="display:flex;gap:4px;align-items:center">
    <input class="edit-input" id="edit-p-${id}" value="${(rec.puerto||'').replace(/"/g,'&quot;')}" style="width:160px">
    <button class="btn-save-cell" onclick="saveEditAlmacen(${id})">Guardar</button>
    <button class="btn btn-secondary" style="padding:3px 10px;font-size:.72rem" onclick="renderMaestro()">Cancelar</button>
  </div>`;
  document.getElementById('edit-n-'+id).focus();
}

async function saveEditAlmacen(id){
  const nombre=document.getElementById('edit-n-'+id).value.trim();
  const puerto=document.getElementById('edit-p-'+id).value.trim();
  if(!nombre){alert('El nombre no puede estar vacio');return;}
  const token=await getValidToken();
  if(!token){alert('Debe iniciar sesion');return;}
  try{
    const r=await fetch(`/almacen/almacenes/${id}`,{
      method:'PUT',headers:{'Content-Type':'application/json','Authorization':'Bearer '+token},
      body:JSON.stringify({nombre,puerto})
    });
    if(!r.ok){const e=await r.json();alert(e.detail||'Error');return;}
    await loadMaestro();
    await loadAlmacenesList();
    showMaestroMsg('Almacen actualizado','#10b981');
  }catch(e){alert('Error: '+e.message);}
}

async function addAlmacen(){
  const inputNombre=document.getElementById('mNuevoNombre');
  const inputPuerto=document.getElementById('mNuevoPuerto');
  const nombre=inputNombre.value.trim();
  const puerto=inputPuerto.value.trim();
  if(!nombre){alert('Ingrese un nombre');return;}
  const token=await getValidToken();
  if(!token){alert('Debe iniciar sesion');return;}
  try{
    const r=await fetch('/almacen/almacenes',{
      method:'POST',headers:{'Content-Type':'application/json','Authorization':'Bearer '+token},
      body:JSON.stringify({nombre,puerto})
    });
    if(!r.ok){const e=await r.json();alert(e.detail||'Error');return;}
    inputNombre.value='';inputPuerto.value='';
    await loadMaestro();
    await loadAlmacenesList();
    showMaestroMsg('Almacen agregado','#10b981');
  }catch(e){alert('Error: '+e.message);}
}

async function deleteAlmacen(id,nombre){
  if(!confirm('Eliminar el almacen "'+nombre+'"?'))return;
  const token=await getValidToken();
  if(!token){alert('Debe iniciar sesion');return;}
  try{
    const r=await fetch(`/almacen/almacenes/${id}`,{
      method:'DELETE',headers:{'Authorization':'Bearer '+token}
    });
    if(!r.ok){const e=await r.json();alert(e.detail||'Error');return;}
    await loadMaestro();
    await loadAlmacenesList();
    showMaestroMsg('Almacen eliminado','#ef4444');
  }catch(e){alert('Error: '+e.message);}
}

async function seedAlmacenes(){
  const token=await getValidToken();
  if(!token){alert('Debe iniciar sesion');return;}
  try{
    const r=await fetch('/almacen/almacenes/seed',{
      method:'POST',headers:{'Authorization':'Bearer '+token}
    });
    if(!r.ok){const e=await r.json();alert(e.detail||'Error');return;}
    const data=await r.json();
    await loadMaestro();
    await loadAlmacenesList();
    showMaestroMsg(`Importados ${data.seeded} almacenes (${data.total_distinct} encontrados en registros)`,'#3b82f6');
  }catch(e){alert('Error: '+e.message);}
}

function showMaestroMsg(msg,color){
  const el=document.getElementById('maestroMsg');
  el.innerHTML=`<div style="padding:8px 16px;background:${color}15;color:${color};border-radius:6px;font-size:.85rem;font-weight:600">${msg}</div>`;
  setTimeout(()=>{el.innerHTML='';},4000);
}
</script></body></html>"""
    content = content.replace("__TODAY__", today_str).replace("__PLUS7__", plus7_str)
    return HTMLResponse(content=content, headers={"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"})


@app.get("/health")
async def health_check():
    return {"status": "healthy"}
