"""Login / logout routes."""

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from starlette.status import HTTP_303_SEE_OTHER

from crowdtrans.web.app import templates
from crowdtrans.web.auth import authenticate

router = APIRouter()


@router.get("/login")
def login_page(request: Request, error: str = ""):
    if request.session.get("user"):
        return RedirectResponse(url="/", status_code=HTTP_303_SEE_OTHER)
    return templates.TemplateResponse("login.html", {
        "request": request,
        "error": error,
    })


@router.post("/login")
async def login_submit(request: Request):
    form = await request.form()
    username = form.get("username", "").strip()
    password = form.get("password", "")

    user = authenticate(username, password)
    if user is None:
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Invalid username or password.",
            "username": username,
        })

    # Store user info in session
    request.session["user"] = {
        "username": user["username"],
        "display_name": user.get("display_name", username),
        "email": user.get("email", ""),
    }

    return RedirectResponse(url="/", status_code=HTTP_303_SEE_OTHER)


@router.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=HTTP_303_SEE_OTHER)
