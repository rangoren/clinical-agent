from fastapi import FastAPI

from routes.calendar_routes import router as calendar_router
from routes.feedback_routes import router as feedback_router
from routes.home_routes import router as home_router
from routes.message_routes import router as message_router
from routes.undo_routes import router as undo_router


app = FastAPI()


app.include_router(home_router)
app.include_router(message_router)
app.include_router(calendar_router)
app.include_router(feedback_router)
app.include_router(undo_router)
