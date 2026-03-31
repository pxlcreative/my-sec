from sqlalchemy import text
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


# Reusable server-side updated_at default — pass as server_default/onupdate
UPDATED_AT_DEFAULT = text("NOW()")
