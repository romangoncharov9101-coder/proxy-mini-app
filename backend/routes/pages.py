import logging
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates


templates = Jinja2Templates(directory='frontend')
logger = logging.getLogger("routes.pages")
router = APIRouter()

@router.get('/', response_class=HTMLResponse)
async def read_index(request: Request):
    return templates.TemplateResponse(request=request, name='index.html', context={'request': request})

@router.get('/403', response_class=HTMLResponse)
async def forbidden_page(request: Request):
    return templates.TemplateResponse(request=request, name='403.html', context={'request': request}, status_code=403)

@router.get('/404', response_class=HTMLResponse)
async def forbidden_page(request: Request):
    return templates.TemplateResponse(request=request, name='404.html', context={'request': request}, status_code=404)