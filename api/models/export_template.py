import datetime

from sqlalchemy import Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base


class ExportTemplate(Base):
    __tablename__ = "export_templates"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    description: Mapped[str | None] = mapped_column(Text)
    format: Mapped[str] = mapped_column(Text, nullable=False)          # csv/json/xlsx
    filter_criteria: Mapped[dict | None] = mapped_column(JSONB)
    field_selection: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime.datetime | None] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime.datetime | None] = mapped_column(
        server_default=func.now(), onupdate=func.now()
    )
