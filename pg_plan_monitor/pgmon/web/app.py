"""Веб-интерфейс: страницы (Jinja2) + JSON API.

Запуск: uvicorn pgmon.web.app:app --host 0.0.0.0 --port 8080
"""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .api import router as api_router

BASE = Path(__file__).parent

app = FastAPI(title="pg_plan_monitor", docs_url="/api/docs")
app.include_router(api_router)
app.mount("/static", StaticFiles(directory=BASE / "static"), name="static")

templates = Jinja2Templates(directory=BASE / "templates")

PAGES = {
    "/": ("dashboard.html", "Дашборд"),
    "/queries": ("queries.html", "Запросы"),
    "/activity": ("activity.html", "Активность"),
    "/locks": ("locks.html", "Блокировки"),
    "/alerts": ("alerts.html", "Алерты"),
    "/recommendations": ("recommendations.html", "Рекомендации"),
}


def render(request: Request, template: str, title: str, **ctx) -> HTMLResponse:
    return templates.TemplateResponse(
        request, template, {"title": title, "path": request.url.path, **ctx}
    )


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    return render(request, "dashboard.html", "Дашборд")


@app.get("/queries", response_class=HTMLResponse)
def queries_page(request: Request):
    return render(request, "queries.html", "Запросы")


@app.get("/query/{queryid}", response_class=HTMLResponse)
def query_page(request: Request, queryid: int):
    return render(request, "query_detail.html", f"Запрос {queryid}", queryid=queryid)


@app.get("/table/{datname}/{table}", response_class=HTMLResponse)
def table_page(request: Request, datname: str, table: str):
    return render(request, "table_detail.html", f"Таблица {table}",
                  datname=datname, table=table)


@app.get("/activity", response_class=HTMLResponse)
def activity_page(request: Request):
    return render(request, "activity.html", "Активность")


@app.get("/locks", response_class=HTMLResponse)
def locks_page(request: Request):
    return render(request, "locks.html", "Блокировки")


@app.get("/alerts", response_class=HTMLResponse)
def alerts_page(request: Request):
    return render(request, "alerts.html", "Алерты")


@app.get("/recommendations", response_class=HTMLResponse)
def recommendations_page(request: Request):
    return render(request, "recommendations.html", "Рекомендации")
