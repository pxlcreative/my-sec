# Import all models here so Alembic's autogenerate can discover them via Base.metadata
from models.base import Base  # noqa: F401
from models.firm import Firm, FirmChange, FirmSnapshot  # noqa: F401
from models.aum import FirmAumHistory  # noqa: F401
from models.brochure import AdvBrochure  # noqa: F401
from models.platform import FirmPlatform, PlatformDefinition  # noqa: F401
from models.custom_property import (  # noqa: F401
    CustomPropertyDefinition,
    FirmCustomProperty,
)
from models.alert import AlertEvent, AlertRule  # noqa: F401
from models.sync_job import SyncJob  # noqa: F401
from models.export_job import ExportJob  # noqa: F401
from models.api_key import ApiKey  # noqa: F401
from models.disclosures import FirmDisclosuresSummary  # noqa: F401
from models.export_template import ExportTemplate  # noqa: F401
from models.cron_schedule import CronSchedule  # noqa: F401
